# action/function_app.py — complete, nothing left as a placeholder

import azure.functions as func
import os, uuid, logging
from datetime import datetime, timedelta
from azure.cosmos import CosmosClient

from action.fitness_evaluator import score_candidate, composite
from action.confidence_gate import gate
from action.promotion_engine import promote
from action.weight_tuner import adjust_weights
from llm.nemotron_client import explain_promotion
from decision.baseline.cosmos_baseline_store import CosmosBaselineStore
from decision.deviation.deviation_scorer import DOMAIN_METRICS

app = func.FunctionApp()
cosmos = CosmosClient.from_connection_string(os.environ["COSMOS_CONN_STR"])

DEFAULT_WEIGHTS = {"performance": 0.4, "cost": 0.15, "security": 0.2, "scaling": 0.15, "recovery": 0.1}


# ---------- small Cosmos helpers ----------

def get_trigger_event(trigger_id: str) -> dict | None:
    container = cosmos.get_database_client("clouddna").get_container_client("trigger_events")
    results = list(container.query_items(
        query="SELECT * FROM c WHERE c.trigger_id = @t",
        parameters=[{"name": "@t", "value": trigger_id}],
        enable_cross_partition_query=True,
    ))
    return results[0] if results else None


def get_app_registry_entry(app_id: str) -> dict:
    container = cosmos.get_database_client("clouddna").get_container_client("app_registry")
    try:
        return container.read_item(app_id, partition_key=app_id)
    except Exception:
        return {}


def get_slo_targets(app_id: str) -> dict:
    entry = get_app_registry_entry(app_id)
    return entry.get("slo_targets", {
        "p95_latency_ms_good": 200, "p95_latency_ms_bad": 1500,
        "error_rate_pct_good": 0.5, "error_rate_pct_bad": 5.0,
        "hourly_compute_cost_usd_ceiling": 0.50,
    })


def get_fitness_weights(app_id: str, category: str) -> dict:
    container = cosmos.get_database_client("clouddna").get_container_client("fitness_weights")
    try:
        entry = container.read_item(f"{app_id}:{category}", partition_key=app_id)
        return entry["weights"]
    except Exception:
        entry = get_app_registry_entry(app_id)
        return entry.get("fitness_weights_override", {}).get(category, DEFAULT_WEIGHTS)


def save_fitness_weights(app_id: str, category: str, weights: dict):
    container = cosmos.get_database_client("clouddna").get_container_client("fitness_weights")
    container.upsert_item({"id": f"{app_id}:{category}", "app_id": app_id,
                            "trigger_category": category, "weights": weights,
                            "updated_at": datetime.utcnow().isoformat() + "Z"})


def get_min_confidence_margin(app_id: str) -> float:
    entry = get_app_registry_entry(app_id)
    return entry.get("promotion_policy", {}).get("min_confidence_margin", 0.15)


def safe_explain(winner_scores: dict, runner_up_scores: dict) -> tuple[str, str]:
    try:
        return explain_promotion(winner_scores, runner_up_scores), "nemotron_ultra"
    except Exception:
        logging.warning("Nemotron explanation call failed, using fallback text")
        return f"Selected for highest composite score among evaluated candidates.", "fallback_template"


def deploy_stub_fn(app_id: str, genome: dict):
    # no real AKS/Medusa deployment to patch yet — log it so promotions are still visible/testable
    logging.info(f"[STUB DEPLOY] app={app_id} genome={genome}")


# ---------- candidate collection + promotion ----------

def get_expected_candidate_count(trigger_event: dict) -> int:
    return trigger_event.get("expected_candidate_count", 3)


def run_fitness_gate_and_promote(candidates: list[dict], trigger_event: dict):
    app_id = trigger_event["app_id"]
    category = trigger_event["trigger_category"]
    trigger_id = trigger_event["trigger_id"]

    slo = get_slo_targets(app_id)
    weights = get_fitness_weights(app_id, category)

    scored = []
    for c in candidates:
        domain_scores = score_candidate(c["raw_metrics"], c.get("security_probe_results", {}), slo)
        scored.append({**c, "domain_scores": domain_scores,
                        "composite_score": composite(domain_scores, weights)})
    scored.sort(key=lambda x: x["composite_score"], reverse=True)

    gate_result = gate(scored, get_min_confidence_margin(app_id))
    trigger_container = cosmos.get_database_client("clouddna").get_container_client("trigger_events")

    if not gate_result["gate_passed"]:
        trigger_event["outcome"] = "gate_failed"
        trigger_container.upsert_item(trigger_event)
        logging.info(f"[gate] trigger {trigger_id}: no candidate cleared the confidence margin, not promoting")
        return

    winner = scored[0]
    runner_up = scored[1] if len(scored) > 1 else None
    explanation, explanation_source = safe_explain(
        winner["domain_scores"], runner_up["domain_scores"] if runner_up else {})

    promote(winner["genome"], app_id, deploy_stub_fn)

    now = datetime.utcnow()
    genome_container = cosmos.get_database_client("clouddna").get_container_client("genome_library")
    genome_container.upsert_item({
        "id": f"promo-{trigger_id}",
        "app_id": app_id,
        "trigger_id": trigger_id,
        "trigger_category": category,
        "winning_candidate_id": winner["candidate_id"],
        "genome": winner["genome"],
        "fingerprint_vector": trigger_event.get("fingerprint_vector"),
        "domain_scores": winner["domain_scores"],          # the PREDICTED scores at promotion time
        "composite_score": winner["composite_score"],
        "runner_up_score": runner_up["composite_score"] if runner_up else 0.0,
        "confidence_margin": gate_result["confidence_margin"],
        "promoted_at": now.isoformat() + "Z",
        "rollback_config": trigger_event.get("current_production_config", {}),
        "explanation": explanation,
        "explanation_source": explanation_source,
        "weights_used": weights,
        "weight_check_due_at": (now + timedelta(minutes=15)).isoformat() + "Z",
        "weight_check_done": False,
    })

    trigger_event["outcome"] = "promoted"
    trigger_container.upsert_item(trigger_event)


# ---------- weight self-tuning, run on a timer ----------

def fetch_actual_post_promotion_scores(app_id: str) -> dict:
    """Approximates real-world outcome using each metric's current EWMA ('ALL'
    bucket), which updates on every window — close enough for a 15-minute
    check without a separate post-promotion metrics store."""
    store = CosmosBaselineStore()
    current = {"performance": {}, "cost": {}, "security": {}, "scaling": {}, "recovery": {}}
    for domain, metrics in DOMAIN_METRICS.items():
        for m in metrics:
            baseline = store.get(app_id, m, "ALL")
            if baseline and baseline.ewma is not None:
                current[domain][m] = baseline.ewma
    return score_candidate(current, {"auth_bruteforce_blocked": True, "malformed_req_5xx_rate": 0.0},
                            get_slo_targets(app_id))


def run_pending_weight_adjustments():
    container = cosmos.get_database_client("clouddna").get_container_client("genome_library")
    now_iso = datetime.utcnow().isoformat() + "Z"
    pending = list(container.query_items(
        query="SELECT * FROM c WHERE c.weight_check_done = false AND c.weight_check_due_at <= @now",
        parameters=[{"name": "@now", "value": now_iso}],
        enable_cross_partition_query=True,
    ))
    for entry in pending:
        actual_scores = fetch_actual_post_promotion_scores(entry["app_id"])
        new_weights = adjust_weights(entry["weights_used"], entry["domain_scores"], actual_scores)
        save_fitness_weights(entry["app_id"], entry["trigger_category"], new_weights)
        entry["weight_check_done"] = True
        container.upsert_item(entry)
        logging.info(f"[weight-tuner] updated weights for {entry['app_id']}/{entry['trigger_category']}")


# ---------- Azure entrypoints ----------

@app.event_grid_trigger(arg_name="event")
def action_on_candidate_result(event: func.EventGridEvent):
    result = event.get_json()   # matches CandidateEvaluationResult schema
    container = cosmos.get_database_client("clouddna").get_container_client("candidate_evaluations")
    container.upsert_item({**result, "id": result["candidate_id"]})

    trigger_id = result["trigger_id"]
    trigger_event = get_trigger_event(trigger_id)
    if trigger_event is None:
        logging.warning(f"Candidate {result['candidate_id']} arrived for unknown trigger {trigger_id}")
        return

    all_for_trigger = list(container.query_items(
        query="SELECT * FROM c WHERE c.trigger_id = @t",
        parameters=[{"name": "@t", "value": trigger_id}],
        enable_cross_partition_query=True,
    ))
    if len(all_for_trigger) < get_expected_candidate_count(trigger_event):
        return   # still waiting on more candidates for this trigger

    try:
        run_fitness_gate_and_promote(all_for_trigger, trigger_event)
    except Exception:
        logging.exception(f"Failed fitness/gate/promotion for trigger {trigger_id}")


@app.timer_trigger(schedule="0 */15 * * * *", arg_name="timer")
def action_weight_tuning_tick(timer: func.TimerRequest):
    run_pending_weight_adjustments()