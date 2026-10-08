"""
function_app.py — CloudDNA Action Layer (Member 3)

ROOT CAUSE FIX: The original code used an in-memory dict (_CANDIDATE_BUFFER) as a
buffer to collect 3 candidate results before running the action pipeline. On Azure
Consumption plans, each EventGrid event can hit a cold-started worker with an empty
buffer — so the 3rd candidate was NEVER seen by the same instance.

FIX: Replaced in-memory buffer with Cosmos DB (container: candidate_evaluations).
Each invocation writes its candidate, then counts how many exist in Cosmos for that
trigger_id. When the count reaches expected_candidate_count, that invocation runs the
full action pipeline. All other invocations return early.
"""
import os
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

try:
    import azure.functions as func
except ImportError:
    func = None

from fitness_evaluator import FitnessEvaluator
from confidence_gate import ConfidenceGate
from promotion_engine import PromotionEngine
from weight_tuner import WeightTunerService

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("clouddna.action")

# ── Cosmos DB ─────────────────────────────────────────────────────────────────
try:
    from azure.cosmos import CosmosClient, exceptions as cosmos_exc
except ImportError:
    CosmosClient = None
    cosmos_exc = None

cosmos_conn_str = os.getenv("COSMOS_CONN_STR")
cosmos_client = None
if cosmos_conn_str and CosmosClient:
    try:
        cosmos_client = CosmosClient.from_connection_string(cosmos_conn_str)
        logger.info("[Action] Cosmos DB client initialized.")
    except Exception as e:
        logger.warning("[Action] Could not initialize CosmosClient: %s", e)

# ── Pipeline components ───────────────────────────────────────────────────────
fitness_evaluator = FitnessEvaluator(cosmos_client=cosmos_client)
confidence_gate   = ConfidenceGate(
    min_confidence_margin=float(os.getenv("MIN_CONFIDENCE_MARGIN", "0.020"))
)
promotion_engine  = PromotionEngine(cosmos_client=cosmos_client)
weight_tuner      = WeightTunerService(cosmos_client=cosmos_client)


# ── Core pipeline ─────────────────────────────────────────────────────────────
def process_action_pipeline(
    trigger_event: Dict[str, Any],
    candidates: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Runs the full Action pipeline once all N candidates are collected.
    Logs every ranking, score, and the final promotion decision.
    """
    app_id         = trigger_event.get("app_id", "medusa")
    trigger_id     = trigger_event.get("trigger_id", "trig-unknown")
    trigger_cat    = trigger_event.get("trigger_category", "TRAFFIC_SPIKE")

    logger.info("[Action] ═══════════════════════════════════════════════════════")
    logger.info("[Action] 🔬 Running Action Pipeline | trigger_id=%s | app=%s | category=%s",
                trigger_id, app_id, trigger_cat)
    logger.info("[Action] ═══════════════════════════════════════════════════════")

    # ── Step 1: Rank candidates by composite fitness ──────────────────────────
    logger.info("[Action] ── STEP 1: Ranking %d candidates by composite fitness ──", len(candidates))
    ranked = fitness_evaluator.rank_candidates(candidates, app_id, trigger_cat)
    if not ranked:
        logger.error("[Action] No valid candidates for evaluation.")
        return {"status": "error", "message": "No valid candidates provided."}

    for i, r in enumerate(ranked):
        ds = r.get("domain_scores", {})
        logger.info(
            "[Action]   Rank #%d — %-30s  composite=%.3f  perf=%.3f  cost=%.3f  sec=%.3f  scale=%.3f  recovery=%.3f",
            i + 1,
            r["candidate_id"],
            r["composite_score"],
            ds.get("performance", 0),
            ds.get("cost", 0),
            ds.get("security", 0),
            ds.get("scaling", 0),
            ds.get("recovery", 0),
        )

    winner    = ranked[0]
    runner_up = ranked[1] if len(ranked) > 1 else None

    # ── Step 2: Confidence Gate ───────────────────────────────────────────────
    logger.info("[Action] ── STEP 2: Evaluating Confidence Gate ──")
    passed, winner, runner_up, margin = confidence_gate.evaluate(ranked)
    logger.info("[Action]   Gate passed=%s  margin=%.3f  winner=%s  runner_up=%s",
                passed, margin,
                winner["candidate_id"],
                runner_up["candidate_id"] if runner_up else "N/A")

    if not passed:
        logger.warning("[Action]   ⚠ Confidence margin %.3f below threshold. Winner NOT promoted.", margin)
    else:
        logger.info("[Action]   ✔ Confidence gate PASSED. Proceeding with promotion.")

    # ── Step 3: Promotion Engine ──────────────────────────────────────────────
    logger.info("[Action] ── STEP 3: Generating Promotion Record ──")
    promotion_record = promotion_engine.promote(
        trigger_event=trigger_event,
        winner=winner,
        runner_up=runner_up,
        confidence_margin=margin,
        gate_passed=passed,
    )

    logger.info(
        "[Action] 🏆 WINNER: candidate=%s  composite=%.3f  gate=%s",
        winner["candidate_id"], winner["composite_score"],
        "PROMOTED ✅" if passed else "GATE REJECTED ❌",
    )
    logger.info("[Action] Promotion record id: %s", promotion_record.get("id"))
    logger.info("[Action] ═══════════════════════════════════════════════════════")

    return {
        "status": "promoted" if passed else "gated_rejected",
        "gate_passed": passed,
        "trigger_id": trigger_id,
        "app_id": app_id,
        "trigger_category": trigger_cat,
        "ranked_candidates": ranked,
        "promotion_record": promotion_record,
    }


# ── Cosmos DB buffering helper ────────────────────────────────────────────────
def _persist_and_count_candidates(candidate_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    1. Upserts this candidate into Cosmos DB (container: candidate_evaluations).
    2. Queries how many candidates exist for this trigger_id.
    3. Returns the full list if >= expected_count, else returns empty list.

    This replaces the broken in-memory dict pattern. Cosmos DB is the shared
    buffer that works correctly across serverless instances.
    """
    trigger_id     = candidate_result.get("trigger_id", "trig-unknown")
    candidate_id   = candidate_result.get("candidate_id", "cand-unknown")
    expected_count = 3  # default; will be overridden from trigger_events if available

    if not cosmos_client:
        logger.warning("[Action] No Cosmos DB — cannot buffer across serverless instances. Running pipeline with 1 candidate.")
        return [candidate_result]

    db = cosmos_client.get_database_client("clouddna")

    # ── Write this candidate ──────────────────────────────────────────────────
    try:
        container = db.get_container_client("candidate_evaluations")
        doc = {**candidate_result, "id": f"{trigger_id}_{candidate_id}"}
        container.upsert_item(doc)
        logger.info("[Action] Buffered candidate %s (trigger=%s) to Cosmos DB.", candidate_id, trigger_id)
    except Exception as exc:
        logger.error("[Action] Failed to buffer candidate %s: %s", candidate_id, exc)
        return []

    # ── Fetch trigger_event for expected_candidate_count ─────────────────────
    trigger_event = None
    try:
        te_container = db.get_container_client("trigger_events")
        items = list(te_container.query_items(
            query="SELECT * FROM c WHERE c.trigger_id = @tid",
            parameters=[{"name": "@tid", "value": trigger_id}],
            enable_cross_partition_query=True,
        ))
        if items:
            trigger_event = items[0]
            expected_count = trigger_event.get("expected_candidate_count", 3)
    except Exception as exc:
        logger.warning("[Action] Could not fetch trigger_events for %s: %s", trigger_id, exc)

    # ── Count how many candidates are buffered so far ─────────────────────────
    try:
        container = db.get_container_client("candidate_evaluations")
        count_items = list(container.query_items(
            query="SELECT * FROM c WHERE c.trigger_id = @tid",
            parameters=[{"name": "@tid", "value": trigger_id}],
            enable_cross_partition_query=True,
        ))
        buffered_count = len(count_items)
    except Exception as exc:
        logger.error("[Action] Failed to count buffered candidates: %s", exc)
        return []

    logger.info("[Action] Candidate buffer status: %d/%d received for trigger_id=%s",
                buffered_count, expected_count, trigger_id)

    if buffered_count < expected_count:
        return []  # Not all candidates have arrived yet; another invocation will finalize

    # ── All candidates received — build trigger_event fallback if needed ──────
    if not trigger_event:
        # Reconstruct a minimal trigger_event from the first candidate's data
        trigger_event = {
            "app_id": candidate_result.get("app_id", "medusa"),
            "trigger_id": trigger_id,
            "trigger_category": "TRAFFIC_SPIKE",
            "current_production_config": {"replica_count": 2, "cpu_cores": 1.0},
        }

    logger.info("[Action] All %d candidates received. Executing Action Pipeline!", expected_count)

    # ── Clean up buffered candidates from Cosmos after collecting them ────────
    deleted = 0
    for item in count_items:
        try:
            pk = item.get("trigger_id") or trigger_id
            container.delete_item(item["id"], partition_key=pk)
            deleted += 1
        except Exception:
            pass  # 404 is fine — Experimentation layer may have already deleted it
    if deleted:
        logger.info("[Action] Cleared %d buffered candidates from Cosmos DB.", deleted)

    # Store trigger_event object alongside candidates for pipeline
    return [{"__trigger_event__": trigger_event}] + count_items


# ── Azure Functions entry points ──────────────────────────────────────────────
if func:
    app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

    @app.route(route="evaluate_candidates", methods=["POST"])
    def action_http_evaluate(req: func.HttpRequest) -> func.HttpResponse:
        """HTTP POST endpoint for direct integration testing of the Action pipeline."""
        try:
            body = req.get_json()
        except Exception as exc:
            return func.HttpResponse(
                json.dumps({"error": f"Invalid JSON payload: {exc}"}),
                status_code=400, mimetype="application/json"
            )

        trigger_evt = body.get("trigger_event", {})
        candidates  = body.get("candidates", [])

        try:
            result = process_action_pipeline(trigger_evt, candidates)
            return func.HttpResponse(
                json.dumps(result, indent=2, default=str),
                status_code=200, mimetype="application/json"
            )
        except Exception as exc:
            logger.exception("[Action] HTTP evaluate pipeline failure.")
            return func.HttpResponse(
                json.dumps({"error": str(exc)}),
                status_code=500, mimetype="application/json"
            )

    @app.event_grid_trigger(arg_name="event")
    def action_on_candidate_eventgrid(event: func.EventGridEvent):
        """
        EventGrid trigger — receives one CandidateEvaluationResult at a time from
        the cdna-candidate-results topic. Uses Cosmos DB as a shared buffer to
        collect all 3 candidates before running the Action pipeline.
        """
        try:
            candidate_payload = event.get_json()
            trigger_id    = candidate_payload.get("trigger_id", "unknown")
            candidate_id  = candidate_payload.get("candidate_id", "unknown")

            logger.info("[Action] EventGrid trigger received. trigger_id=%s  candidate_id=%s",
                        trigger_id, candidate_id)

            items = _persist_and_count_candidates(candidate_payload)

            if not items:
                return  # Still waiting for more candidates

            # items[0] is the trigger_event sentinel we injected
            trigger_event = items[0].get("__trigger_event__", {})
            candidates    = [i for i in items[1:] if "__trigger_event__" not in i]

            process_action_pipeline(trigger_event, candidates)

        except Exception:
            logger.exception("[Action] ✘ Unhandled exception in action_on_candidate_eventgrid.")
            raise  # Re-raise so EventGrid retries delivery