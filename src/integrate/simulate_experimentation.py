# integration/simulate_experimentation.py
#
# HARNESS — stands in for Member 2's real Experimentation Layer.
# Subscribes to cdna-trigger-events, fabricates N fake candidates (N read from
# trigger_event["expected_candidate_count"], not hardcoded — see note above),
# publishes them to cdna-candidate-results.
#
# DELETE THIS FILE once Member 2's real Experimentation Layer is pointed at the
# same two topics. Nothing in decision/function_app.py or action/function_app.py
# needs to change when that happens.

import azure.functions as func
import os, copy, logging
from datetime import datetime
from azure.eventgrid import EventGridPublisherClient, EventGridEvent
from azure.core.credentials import AzureKeyCredential

app = func.FunctionApp()

candidate_publisher = EventGridPublisherClient(
    os.environ["CANDIDATE_RESULTS_TOPIC_ENDPOINT"],
    AzureKeyCredential(os.environ["CANDIDATE_RESULTS_TOPIC_KEY"]),
)


def fabricate_raw_metrics(base_deviated_metrics: dict, improvement_factor: float) -> dict:
    """Takes the bad metrics from the trigger and scales them toward 'healthy' by
    improvement_factor (0 = no change, 1 = fully healthy). Deliberately crude —
    this exists to give the Action Layer something plausible to score, not to
    demonstrate real optimization quality."""
    m = copy.deepcopy(base_deviated_metrics)
    perf = m.get("performance", {})
    for key in ["p50_latency_ms", "p95_latency_ms", "p99_latency_ms", "error_rate_pct",
                "cpu_utilization_pct", "queue_depth"]:
        if key in perf:
            healthy_floor = perf[key] * 0.25          # assume healthy is ~75% better than the bad reading
            perf[key] = perf[key] - (perf[key] - healthy_floor) * improvement_factor

    cost = m.get("cost", {})
    if "hourly_compute_cost_usd" in cost:
        # more replicas/cpu costs more — cost gets *worse* as improvement_factor rises
        cost["hourly_compute_cost_usd"] = cost["hourly_compute_cost_usd"] * (1 + 0.6 * improvement_factor)

    scaling = m.get("scaling", {})
    if "cold_start_time_ms" in scaling:
        scaling["cold_start_time_ms"] = max(500, scaling["cold_start_time_ms"] * (1 - 0.5 * improvement_factor))

    return m


def mutate_genome(base_config: dict, step: int) -> dict:
    """step=0 returns the unchanged current production config (the 'do nothing'
    candidate); step>0 scales replicas/cpu up progressively."""
    g = copy.deepcopy(base_config)
    g["replica_count"] = g.get("replica_count", 2) + (step * 2)
    g["cpu_cores"] = round(g.get("cpu_cores", 1.0) + (step * 0.5), 2)
    if step > 0:
        g["redis_cache_enabled"] = True
        g["redis_cache_ttl_seconds"] = 120
    return g


@app.event_grid_trigger(arg_name="event")
def simulate_experimentation(event: func.EventGridEvent):
    trigger_event = event.get_json()   # matches TriggerEvent schema
    trigger_id = trigger_event["trigger_id"]
    app_id = trigger_event["app_id"]
    expected_count = trigger_event.get("expected_candidate_count", 3)
    base_config = trigger_event.get("current_production_config", {})
    bad_metrics = trigger_event.get("deviated_metrics", {})

    logging.info(f"[harness] generating {expected_count} fake candidates for {trigger_id}")

    for step in range(expected_count):
        improvement_factor = min(1.0, step / max(1, expected_count - 1))   # 0.0 -> 1.0 across candidates
        candidate = {
            "trigger_id": trigger_id,
            "candidate_id": f"fake-cand-{trigger_id}-{step}",
            "genome": mutate_genome(base_config, step),
            "generation_method": "harness_mutation",
            "readiness_ts": datetime.utcnow().isoformat() + "Z",
            "raw_metrics": fabricate_raw_metrics(bad_metrics, improvement_factor),
            "security_probe_results": {
                "auth_bruteforce_blocked": True,
                "malformed_req_5xx_rate": 0.0,
            },
        }
        candidate_publisher.send(EventGridEvent(
            event_type="CloudDNA.CandidateEvaluationResult",
            data=candidate,
            subject=f"candidate/{app_id}",
            data_version="1.0",
        ))