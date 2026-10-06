import os
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

try:
    import azure.functions as func
except ImportError:
    func = None

from .fitness_evaluator import FitnessEvaluator
from .confidence_gate import ConfidenceGate
from .promotion_engine import PromotionEngine
from .weight_tuner import WeightTunerService

logger = logging.getLogger("clouddna.action")
logging.basicConfig(level=logging.INFO)

try:
    from azure.cosmos import CosmosClient
except ImportError:
    CosmosClient = None

cosmos_conn_str = os.getenv("COSMOS_CONN_STR")
cosmos_client = None
if cosmos_conn_str and CosmosClient:
    try:
        cosmos_client = CosmosClient.from_connection_string(cosmos_conn_str)
        logger.info("Action Service connected to Azure Cosmos DB.")
    except Exception as e:
        logger.warning("Could not initialize CosmosClient in Action Service: %s", e)

fitness_evaluator = FitnessEvaluator(cosmos_client=cosmos_client)
confidence_gate = ConfidenceGate(min_confidence_margin=float(os.getenv("MIN_CONFIDENCE_MARGIN", "0.15")))
promotion_engine = PromotionEngine(cosmos_client=cosmos_client)
weight_tuner = WeightTunerService(cosmos_client=cosmos_client)

_CANDIDATE_BUFFER: Dict[str, List[Dict[str, Any]]] = {}


def process_action_pipeline(
    trigger_event: Dict[str, Any],
    candidates: List[Dict[str, Any]],
) -> Dict[str, Any]:
    app_id = trigger_event.get("app_id", "medusa")
    trigger_category = trigger_event.get("trigger_category", "TRAFFIC_SPIKE")

    ranked = fitness_evaluator.rank_candidates(candidates, app_id, trigger_category)
    if not ranked:
        return {"status": "error", "message": "No valid candidates provided for evaluation."}

    passed, winner, runner_up, margin = confidence_gate.evaluate(ranked)

    promotion_record = promotion_engine.promote(
        trigger_event=trigger_event,
        winner=winner,
        runner_up=runner_up,
        confidence_margin=margin,
        gate_passed=passed,
    )

    return {
        "status": "promoted" if passed else "gated_rejected",
        "gate_passed": passed,
        "ranked_candidates": ranked,
        "promotion_record": promotion_record,
    }


def handle_candidate_result(candidate_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    trigger_id = candidate_result.get("trigger_id")
    if not trigger_id:
        return None

    if trigger_id not in _CANDIDATE_BUFFER:
        _CANDIDATE_BUFFER[trigger_id] = []

    _CANDIDATE_BUFFER[trigger_id].append(candidate_result)

    expected_count = 3
    trigger_event = None

    if cosmos_client:
        try:
            container = cosmos_client.get_database_client("clouddna").get_container_client("trigger_events")
            query = "SELECT * FROM c WHERE c.trigger_id = @tid"
            items = list(container.query_items(query=query, parameters=[{"name": "@tid", "value": trigger_id}], enable_cross_partition_query=True))
            if items:
                trigger_event = items[0]
                expected_count = trigger_event.get("expected_candidate_count", 3)
        except Exception as e:
            logger.warning("Could not query trigger_events in handle_candidate_result: %s", e)

    if not trigger_event:
        trigger_event = {
            "app_id": "medusa",
            "trigger_id": trigger_id,
            "trigger_category": "TRAFFIC_SPIKE",
            "current_production_config": {"replica_count": 2, "cpu_cores": 1.0},
        }

    if len(_CANDIDATE_BUFFER[trigger_id]) >= expected_count:
        logger.info("All %d candidates received for trigger %s. Executing Action Pipeline!", expected_count, trigger_id)
        candidates_to_eval = _CANDIDATE_BUFFER.pop(trigger_id)
        return process_action_pipeline(trigger_event, candidates_to_eval)

    logger.info("Buffered candidate %s for trigger %s (%d/%d received).",
                candidate_result.get("candidate_id"), trigger_id, len(_CANDIDATE_BUFFER[trigger_id]), expected_count)
    return None


if func:
    app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

    @app.route(route="evaluate_candidates", methods=["POST"])
    def action_http_evaluate(req: func.HttpRequest) -> func.HttpResponse:
        try:
            body = req.get_json()
        except Exception as e:
            return func.HttpResponse(json.dumps({"error": f"Invalid JSON payload: {e}"}), status_code=400, mimetype="application/json")

        trigger_evt = body.get("trigger_event", {})
        candidates = body.get("candidates", [])

        result = process_action_pipeline(trigger_evt, candidates)
        return func.HttpResponse(json.dumps(result, indent=2), status_code=200, mimetype="application/json")

    @app.event_grid_trigger(arg_name="event")
    def action_on_candidate_eventgrid(event: func.EventGridEvent):
        try:
            candidate_payload = event.get_json()
            handle_candidate_result(candidate_payload)
        except Exception:
            logger.exception("Failed processing candidate evaluation in Event Grid trigger.")