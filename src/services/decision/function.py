import azure.functions as func
import json, uuid, os, logging
from datetime import datetime
from azure.eventgrid import EventGridPublisherClient, EventGridEvent
from azure.core.credentials import AzureKeyCredential
from azure.cosmos import CosmosClient

from baseline.cosmos_baseline_store import CosmosBaselineStore
from deviation.deviation_scorer import score_domain, DebounceTracker, DOMAIN_METRICS
from trigger.trigger_classifier import classify, severity_from_zscores, forecast_throughput
from trigger.fingerprint import build_fingerprint
from genome_library.cosmos_client import get_genome_library
from genome_library.fast_path import find_fast_path

app = func.FunctionApp()

# ---- module-level singletons: created once per host instance, reused across invocations ----
cosmos = CosmosClient.from_connection_string(os.environ["COSMOS_CONN_STR"])
baseline_store = CosmosBaselineStore()
debounce = DebounceTracker()

trigger_publisher = EventGridPublisherClient(
    os.environ["TRIGGER_EVENTS_TOPIC_ENDPOINT"],
    AzureKeyCredential(os.environ["TRIGGER_EVENTS_TOPIC_KEY"]),
)
candidate_publisher = EventGridPublisherClient(
    os.environ["CANDIDATE_RESULTS_TOPIC_ENDPOINT"],
    AzureKeyCredential(os.environ["CANDIDATE_RESULTS_TOPIC_KEY"]),
)


def get_expected_candidate_count(app_id: str) -> int:
    """Reads sandbox.max_concurrent_candidates from the app's registered cdna.yaml."""
    container = cosmos.get_database_client("clouddna").get_container_client("app_registry")
    try:
        entry = container.read_item(app_id, partition_key=app_id)
        return entry.get("sandbox", {}).get("max_concurrent_candidates", 3)
    except Exception:
        return 3   # sane fallback if the app isn't registered yet


def process_stream_a_window(window: dict) -> dict:
    app_id = window["app_id"]
    hour = datetime.fromisoformat(window["ts"].replace("Z", "+00:00")).hour

    domain_scores, confirmed_domains = {}, set()
    for domain in DOMAIN_METRICS:
        # feed the baseline first, so it keeps learning even from non-deviated windows
        for metric in DOMAIN_METRICS[domain]:
            val = window.get(domain, {}).get(metric)
            if val is not None:
                baseline_store.update(app_id, metric, hour, val)

        z, deviated_now = score_domain(app_id, domain, window.get(domain, {}), hour, baseline_store)
        domain_scores[domain] = z
        if debounce.observe(app_id, domain, deviated_now):
            confirmed_domains.add(domain)

    if not confirmed_domains:
        return {"status": "no_trigger"}

    fingerprint = build_fingerprint(domain_scores)
    genome_entries = get_genome_library(app_id, cosmos)
    fast_path = find_fast_path(fingerprint, genome_entries)

    trigger_id = f"trig-{uuid.uuid4().hex[:12]}"
    trigger_event = {
        "app_id": app_id,
        "trigger_id": trigger_id,
        "timestamp": window["ts"],
        "trigger_category": classify(confirmed_domains),
        "severity": severity_from_zscores(domain_scores),
        "forecasted_load": forecast_throughput([window]),
        "fingerprint_vector": fingerprint,
        "fingerprint_version": "v1",
        "current_production_config": window.get("current_production_config", {}),
        "deviated_metrics": window,
        "fast_path_match": fast_path,
        "expected_candidate_count": 1 if fast_path["found"] else get_expected_candidate_count(app_id),
    }

    # persist so Action Layer's buffering logic has something to check candidate counts against
    cosmos.get_database_client("clouddna").get_container_client("trigger_events").upsert_item(
        {**trigger_event, "id": trigger_id}
    )

    return {"status": "triggered", **trigger_event}


@app.event_grid_trigger(arg_name="event")
def decision_on_stream_a(event: func.EventGridEvent):
    window = event.get_json()   # matches StreamAWindow schema
    try:
        result = process_stream_a_window(window)
    except Exception:
        logging.exception(f"Failed processing Stream A window for app {window.get('app_id')}")
        return

    if result["status"] == "no_trigger":
        return

    if result["fast_path_match"]["found"]:
        # skip Experimentation entirely — re-apply the historically winning genome directly
        matched = result["fast_path_match"]
        candidate_publisher.send(EventGridEvent(
            event_type="CloudDNA.CandidateEvaluationResult",
            data={
                "trigger_id": result["trigger_id"],
                "candidate_id": f"fastpath-{matched['matched_genome_id']}",
                "genome": matched.get("genome", {}),
                "generation_method": "historical_reuse",
                "readiness_ts": datetime.utcnow().isoformat() + "Z",
                "raw_metrics": {},
                "security_probe_results": {},
            },
            subject=f"candidate/{result['app_id']}",
            data_version="1.0",
        ))
    else:
        trigger_publisher.send(EventGridEvent(
            event_type="CloudDNA.TriggerEvent",
            data=result,
            subject=f"trigger/{result['app_id']}",
            data_version="1.0",
        ))