import os
import json
import uuid
import logging
from datetime import datetime
from typing import Dict, Any, Optional

try:
    import azure.functions as func
except ImportError:
    func = None

from ..shared.schema.decision_models import (
    TriggerCategory,
    SeverityLevel,
)
from .baseline.cosmos_baseline_store import CosmosBaselineStore
from .deviation.deviation_scorer import score_domain, DebounceTracker, DOMAIN_METRICS
from .trigger.trigger_classifier import classify, severity_from_zscores, forecast_throughput
from .trigger.fingerprint import build_fingerprint
from .genome_library.cosmos_client import GenomeLibraryClient
from .genome_library.fast_path import find_fast_path

logger = logging.getLogger("clouddna.decision")
logging.basicConfig(level=logging.INFO)

try:
    from azure.cosmos import CosmosClient
except ImportError:
    CosmosClient = None

try:
    from azure.eventgrid import EventGridPublisherClient, EventGridEvent
    from azure.core.credentials import AzureKeyCredential
except ImportError:
    EventGridPublisherClient = None
    EventGridEvent = None
    AzureKeyCredential = None

# Cloud connections with local fallbacks
cosmos_conn_str = os.getenv("COSMOS_CONN_STR")
cosmos_client = None
if cosmos_conn_str and CosmosClient:
    try:
        cosmos_client = CosmosClient.from_connection_string(cosmos_conn_str)
        logger.info("Decision Service connected to Azure Cosmos DB.")
    except Exception as e:
        logger.warning("Could not initialize CosmosClient: %s", e)

baseline_store = CosmosBaselineStore(cosmos_client=cosmos_client)
genome_library = GenomeLibraryClient(cosmos_client=cosmos_client)
debounce_tracker = DebounceTracker(consecutive_threshold=int(os.getenv("DEBOUNCE_THRESHOLD", "2")))

# Event Grid publisher initialization
trigger_topic_endpoint = os.getenv("TRIGGER_EVENTS_TOPIC_ENDPOINT")
trigger_topic_key = os.getenv("TRIGGER_EVENTS_TOPIC_KEY")
trigger_publisher = None
if trigger_topic_endpoint and trigger_topic_key and EventGridPublisherClient and AzureKeyCredential:
    try:
        trigger_publisher = EventGridPublisherClient(trigger_topic_endpoint, AzureKeyCredential(trigger_topic_key))
    except Exception as e:
        logger.warning("Failed to initialize trigger_publisher: %s", e)

candidate_topic_endpoint = os.getenv("CANDIDATE_RESULTS_TOPIC_ENDPOINT")
candidate_topic_key = os.getenv("CANDIDATE_RESULTS_TOPIC_KEY")
candidate_publisher = None
if candidate_topic_endpoint and candidate_topic_key and EventGridPublisherClient and AzureKeyCredential:
    try:
        candidate_publisher = EventGridPublisherClient(candidate_topic_endpoint, AzureKeyCredential(candidate_topic_key))
    except Exception as e:
        logger.warning("Failed to initialize candidate_publisher: %s", e)


def get_expected_candidate_count(app_id: str) -> int:
    if cosmos_client:
        try:
            container = cosmos_client.get_database_client("clouddna").get_container_client("app_registry")
            entry = container.read_item(app_id, partition_key=app_id)
            return entry.get("sandbox", {}).get("max_concurrent_candidates", 3)
        except Exception:
            pass
    return 3


def process_stream_a_window(window: Dict[str, Any]) -> Dict[str, Any]:
    app_id = window.get("app_id", "medusa")
    ts_str = window.get("ts", datetime.utcnow().isoformat() + "Z")

    try:
        hour = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).hour
    except Exception:
        hour = datetime.utcnow().hour

    domain_scores: Dict[str, float] = {}
    confirmed_domains = set()

    for domain, metrics in DOMAIN_METRICS.items():
        domain_payload = window.get(domain, {})

        for metric in metrics:
            val = domain_payload.get(metric)
            if val is not None:
                try:
                    baseline_store.update(app_id, metric, hour, float(val))
                except (ValueError, TypeError):
                    continue

        z_score, is_deviated = score_domain(app_id, domain, domain_payload, hour, baseline_store)
        domain_scores[domain] = z_score

        if debounce_tracker.observe(app_id, domain, is_deviated):
            confirmed_domains.add(domain)

    if not confirmed_domains:
        return {
            "status": "normal",
            "app_id": app_id,
            "domain_scores": domain_scores,
            "message": "Telemetry within acceptable baseline parameters.",
        }

    fingerprint = build_fingerprint(domain_scores)
    category = classify(confirmed_domains, window)
    severity = severity_from_zscores(domain_scores)
    load_forecast = forecast_throughput([window], horizon_sec=90)

    historical_genomes = genome_library.get_genome_library(app_id)
    similarity_threshold = float(os.getenv("FAST_PATH_SIMILARITY_THRESHOLD", "0.90"))
    fast_path_result = find_fast_path(fingerprint, historical_genomes, similarity_threshold=similarity_threshold)

    trigger_id = f"trig-{datetime.utcnow().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6]}"
    expected_candidates = 1 if fast_path_result["found"] else get_expected_candidate_count(app_id)

    trigger_event = {
        "app_id": app_id,
        "trigger_id": trigger_id,
        "timestamp": ts_str,
        "trigger_category": category.value if isinstance(category, TriggerCategory) else category,
        "severity": severity.value if isinstance(severity, SeverityLevel) else severity,
        "forecasted_load": load_forecast,
        "fingerprint_vector": fingerprint,
        "fingerprint_version": "v1",
        "current_production_config": window.get("current_production_config", {
            "cpu_cores": 1.0,
            "memory_gb": 1.0,
            "replica_count": 2,
            "node_thread_pool_size": 20,
            "redis_cache_enabled": False,
            "redis_cache_ttl_seconds": 0,
            "rate_limiting_enabled": False,
            "health_check_timeout_sec": 2,
        }),
        "deviated_metrics": window,
        "fast_path_match": fast_path_result,
        "expected_candidate_count": expected_candidates,
    }

    if cosmos_client:
        try:
            container = cosmos_client.get_database_client("clouddna").get_container_client("trigger_events")
            container.upsert_item({**trigger_event, "id": trigger_id})
            logger.info("Persisted TriggerEvent %s to Cosmos DB.", trigger_id)
        except Exception as e:
            logger.error("Failed to persist TriggerEvent to Cosmos DB: %s", e)

    _dispatch_event(trigger_event)
    debounce_tracker.reset(app_id)

    return {
        "status": "triggered",
        "trigger_event": trigger_event,
    }


def _dispatch_event(trigger_event: Dict[str, Any]):
    app_id = trigger_event["app_id"]
    is_fast_path = trigger_event["fast_path_match"]["found"]

    if is_fast_path:
        logger.info("Fast-path match detected! Emitting directly to Candidate Evaluation topic.")
        if candidate_publisher and EventGridEvent:
            try:
                candidate_publisher.send(EventGridEvent(
                    event_type="CloudDNA.CandidateEvaluationResult",
                    data={
                        "trigger_id": trigger_event["trigger_id"],
                        "candidate_id": f"fastpath-{trigger_event['fast_path_match']['matched_genome_id']}",
                        "genome": trigger_event["fast_path_match"].get("genome", {}),
                        "generation_method": "fast_path_reuse",
                        "readiness_ts": datetime.utcnow().isoformat() + "Z",
                        "raw_metrics": trigger_event["deviated_metrics"],
                        "security_probe_results": {"auth_bruteforce_blocked": True, "malformed_req_5xx_rate": 0.0},
                    },
                    subject=f"candidate/{app_id}",
                    data_version="1.0",
                ))
            except Exception as e:
                logger.error("Failed to dispatch fast-path candidate event: %s", e)
    else:
        logger.info("Dispatching TriggerEvent to Experimentation Layer via Event Grid.")
        if trigger_publisher and EventGridEvent:
            try:
                trigger_publisher.send(EventGridEvent(
                    event_type="CloudDNA.TriggerEvent",
                    data=trigger_event,
                    subject=f"trigger/{app_id}",
                    data_version="1.0",
                ))
            except Exception as e:
                logger.error("Failed to dispatch TriggerEvent to Event Grid: %s", e)


if func:
    app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

    @app.route(route="stream_a", methods=["POST"])
    def decision_http_stream_a(req: func.HttpRequest) -> func.HttpResponse:
        """
        HTTP endpoint for local execution or webhook ingestion: POST /api/stream_a
        """
        try:
            body = req.get_json()
        except Exception as e:
            return func.HttpResponse(
                json.dumps({"error": f"Invalid JSON payload: {str(e)}"}),
                status_code=400,
                mimetype="application/json",
            )

        result = process_stream_a_window(body)
        return func.HttpResponse(
            json.dumps(result, indent=2),
            status_code=200 if result.get("status") in ["normal", "triggered"] else 500,
            mimetype="application/json",
        )

    @app.event_grid_trigger(arg_name="event")
    def decision_on_stream_a_eventgrid(event: func.EventGridEvent):
        """
        Production Event Grid Trigger from Member 1's Observation telemetry.
        """
        try:
            window_payload = event.get_json()
            process_stream_a_window(window_payload)
        except Exception:
            logger.exception("Failed processing Stream A window in Event Grid trigger.")