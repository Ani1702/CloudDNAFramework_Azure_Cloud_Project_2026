## TODO: change this developer driven scoring to use the genome_library baseline scores. 

def normalize(value: float, good: float, bad: float) -> float:
    """Linearly maps value to a 0-1 score, where `good` -> 1.0 and `bad` -> 0.0.
    Clips outside that range. Works for metrics where lower is better."""
    if bad == good:
        return 1.0
    score = (bad - value) / (bad - good)
    return max(0.0, min(1.0, score))

def score_candidate(raw_metrics: dict, security_probes: dict, slo: dict) -> dict:
    perf = raw_metrics["performance"]
    perf_score = (
        normalize(perf["p95_latency_ms"], slo["p95_latency_ms_good"], slo["p95_latency_ms_bad"]) * 0.5
        + normalize(perf["error_rate_pct"], slo["error_rate_pct_good"], slo["error_rate_pct_bad"]) * 0.3
        + normalize(perf["cpu_utilization_pct"], 60, 95) * 0.2
    )
    cost_score = normalize(raw_metrics["cost"]["hourly_compute_cost_usd"], 0, slo["hourly_compute_cost_usd_ceiling"])
    security_score = 1.0 if security_probes.get("auth_bruteforce_blocked") and \
        security_probes.get("malformed_req_5xx_rate", 1) == 0 else 0.4
    scaling_score = normalize(raw_metrics["scaling"]["cold_start_time_ms"], 500, 5000)
    recovery = raw_metrics["recovery"]
    recovery_score = 1.0 if recovery["restart_count"] == 0 and recovery["health_check_failure_rate_pct"] == 0 else 0.3

    return {"performance": round(perf_score, 3), "cost": round(cost_score, 3),
            "security": round(security_score, 3), "scaling": round(scaling_score, 3),
            "recovery": round(recovery_score, 3)}

def composite(domain_scores: dict, weights: dict) -> float:
    return round(sum(domain_scores[d] * weights[d] for d in domain_scores), 3)