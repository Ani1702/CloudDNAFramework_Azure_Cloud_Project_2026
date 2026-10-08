"""
function_app.py — CloudDNA Experimentation Layer (Member 2)

Entry points:
  - on_trigger_event  : EventGrid trigger (called by Decision Layer via cdna-trigger-events topic)
  - http_run_experimentation : HTTP POST for direct integration testing
"""
import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any

try:
    import azure.functions as func
except ImportError:
    func = None

from candidate_generator import CandidateGenerator
from sandbox_orchestrator import SandboxOrchestrator
from traffic_dispatcher import TrafficDispatcher
from evaluation_coordinator import EvaluationCoordinator

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("clouddna.experimentation")

# ── Cosmos DB client ──────────────────────────────────────────────────────────
try:
    from azure.cosmos import CosmosClient
except ImportError:
    CosmosClient = None

cosmos_conn_str = os.getenv("COSMOS_CONN_STR")
cosmos_client = (
    CosmosClient.from_connection_string(cosmos_conn_str)
    if (cosmos_conn_str and CosmosClient)
    else None
)
if cosmos_client:
    logger.info("[Experimentation] Cosmos DB client initialized.")
else:
    logger.warning("[Experimentation] Cosmos DB unavailable — genome seeding disabled.")

# ── Pipeline component singletons ─────────────────────────────────────────────
generator    = CandidateGenerator(cosmos_client=cosmos_client)
orchestrator = SandboxOrchestrator()
dispatcher   = TrafficDispatcher()
coordinator  = EvaluationCoordinator(cosmos_client=cosmos_client)


# ─────────────────────────────────────────────────────────────────────────────
def run_experimentation_workflow(trigger_event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Full CloudDNA Member 2 Experimentation pipeline:

      [0]  Fast-path check     — skip if Decision Layer found a cached genome
      [1]  Candidate generation — Optuna + Nemotron priors  (3 candidates)
      [2]  Sandbox deployment  — real ACI containers, concurrent (Novelty #1)
      [3]  Traffic replay      — lock-step probe + security injection (Novelty #2,4)
      [4]  Teardown            — delete sandbox ACI containers
      [5]  Publish results     — Cosmos DB + cdna-candidate-results Event Grid topic
    """
    start_ts  = datetime.now(timezone.utc).isoformat()
    app_id    = trigger_event.get("app_id", "medusa")
    trigger_id = trigger_event.get("trigger_id", "trig-unknown")
    trigger_cat = trigger_event.get("trigger_category", "UNKNOWN")

    logger.info(
        "[Experimentation] ════════════════════════════════════════════════════"
    )
    logger.info(
        "[Experimentation] 🚀 Received TriggerEvent | trigger_id=%s | app=%s | category=%s",
        trigger_id, app_id, trigger_cat
    )
    logger.info(
        "[Experimentation] ════════════════════════════════════════════════════"
    )

    # ── [0] Fast-path check ───────────────────────────────────────────────────
    fast_path = trigger_event.get("fast_path_match", {})
    if fast_path.get("found", False):
        matched = fast_path.get("matched_genome_id", "unknown")
        logger.info("[Experimentation] ⚡ Fast-path hit (genome=%s). Skipping full experiment.", matched)
        return {"status": "skipped", "reason": "fast_path_match", "matched_genome_id": matched}

    logger.info("[Experimentation] No fast-path match. Proceeding with full experimentation pipeline.")

    # ── [1] Fetch historical genomes for seeding ──────────────────────────────
    historical_genomes = []
    if cosmos_client:
        try:
            container = cosmos_client.get_database_client("clouddna").get_container_client("genome_library")
            query = "SELECT TOP 3 * FROM c WHERE c.app_id = @app_id ORDER BY c._ts DESC"
            historical_genomes = list(
                container.query_items(
                    query=query,
                    parameters=[{"name": "@app_id", "value": app_id}],
                    enable_cross_partition_query=False,
                )
            )
            logger.info("[Experimentation] Fetched %d historical genomes for seeding.", len(historical_genomes))
        except Exception as exc:
            logger.warning("[Experimentation] Could not fetch historical genomes: %s", exc)

    # ── [2] Candidate Generation (Optuna + Nemotron) ──────────────────────────
    logger.info("[Experimentation] ── STEP 1: Generating 3 candidate configurations ──")
    candidates = generator.generate_candidates(trigger_event, historical_genomes, num_candidates=3)
    logger.info("[Experimentation] ✔ Generated %d candidates:", len(candidates))
    for c in candidates:
        logger.info(
            "[Experimentation]   candidate_id=%-30s  method=%-25s  genome=%s",
            c["candidate_id"], c["generation_method"], json.dumps(c["genome"])
        )

    # ── [3] Sandbox Deployment & Readiness Gating (Novelty #1) ───────────────
    logger.info("[Experimentation] ── STEP 2: Deploying candidates to ACI sandbox ──")
    synced_candidates = orchestrator.deploy_and_synchronize(candidates, app_id=app_id)

    # ── [4] Traffic Replay & Security Probing (Novelty #2 & #4) ──────────────
    logger.info("[Experimentation] ── STEP 3: Replaying traffic + security probes ──")
    evaluated_candidates = dispatcher.replay_and_probe(synced_candidates)
    for c in evaluated_candidates:
        m = c.get("raw_metrics", {})
        logger.info(
            "[Experimentation]   candidate=%-30s  p50=%.1fms  p95=%.1fms  rps=%.0f  err=%.2f%%  cost=$%.3f/hr  sec_blocked=%s",
            c["candidate_id"],
            m.get("p50_latency_ms", 0),
            m.get("p95_latency_ms", 0),
            m.get("throughput_rps", 0),
            m.get("error_rate_pct", 0),
            m.get("hourly_compute_cost_usd", 0),
            c.get("security_probe_results", {}).get("auth_bruteforce_blocked", False),
        )

    # ── [5] Teardown Sandbox ──────────────────────────────────────────────────
    logger.info("[Experimentation] ── STEP 4: Tearing down sandbox containers ──")
    orchestrator.teardown_sandbox(evaluated_candidates)

    # ── [6] Publish Results to Action Layer ───────────────────────────────────
    logger.info("[Experimentation] ── STEP 5: Publishing results to cdna-candidate-results ──")
    coordinator.publish_candidate_results(evaluated_candidates, app_id=app_id)

    end_ts = datetime.now(timezone.utc).isoformat()
    logger.info(
        "[Experimentation] ════ Pipeline COMPLETE | trigger_id=%s | candidates=%d | start=%s | end=%s ════",
        trigger_id, len(evaluated_candidates), start_ts, end_ts
    )

    return {
        "status": "completed",
        "trigger_id": trigger_id,
        "app_id": app_id,
        "trigger_category": trigger_cat,
        "candidates_evaluated": len(evaluated_candidates),
        "pipeline_start_ts": start_ts,
        "pipeline_end_ts": end_ts,
        "aci_available": getattr(orchestrator, "aci_client", None) is not None,
        "aci_init_error": getattr(orchestrator, "aci_init_error", None),
        "results": evaluated_candidates,
    }


# ── Azure Functions entry points ──────────────────────────────────────────────
if func:
    app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

    @app.event_grid_trigger(arg_name="event")
    def on_trigger_event(event: func.EventGridEvent):
        """
        EventGrid trigger — called by the Decision Layer when a TriggerEvent
        is published to the `cdna-trigger-events` topic.
        """
        try:
            trigger_payload = event.get_json()
            logger.info("[Experimentation] EventGrid trigger received. event_type=%s", event.event_type)
            run_experimentation_workflow(trigger_payload)
        except Exception:
            logger.exception("[Experimentation] ✘ Unhandled exception in on_trigger_event.")
            raise   # Re-raise so Azure retries delivery per the retry policy

    @app.route(route="run_experimentation", methods=["POST"])
    def http_run_experimentation(req: func.HttpRequest) -> func.HttpResponse:
        """HTTP POST endpoint for direct integration testing (bypasses EventGrid)."""
        try:
            body = req.get_json()
        except Exception as exc:
            return func.HttpResponse(
                json.dumps({"error": f"Invalid JSON body: {exc}"}),
                status_code=400,
                mimetype="application/json",
            )

        try:
            result = run_experimentation_workflow(body)
            return func.HttpResponse(
                json.dumps(result, indent=2, default=str),
                status_code=200,
                mimetype="application/json",
            )
        except Exception as exc:
            logger.exception("[Experimentation] ✘ HTTP trigger pipeline failure.")
            return func.HttpResponse(
                json.dumps({"error": str(exc)}),
                status_code=500,
                mimetype="application/json",
            )