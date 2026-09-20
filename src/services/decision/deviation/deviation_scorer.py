DOMAIN_METRICS = {
    "performance": ["p50_latency_ms", "p95_latency_ms", "p99_latency_ms",
                     "throughput_rps", "error_rate_pct", "cpu_utilization_pct",
                     "memory_utilization_pct", "queue_depth"],
    "cost": ["hourly_compute_cost_usd"],
    "security": ["failed_auth_attempts_per_min", "anomalous_ip_request_rate"],
    "scaling": ["cold_start_time_ms"],
    "recovery": ["restart_count", "health_check_failure_rate_pct"],
}

DEVIATION_Z_THRESHOLD = 3.5     
DEBOUNCE_WINDOWS = 3            

def robust_z(value: float, baseline) -> float:
    if baseline is None or baseline.ewma is None or baseline.mad() is None:
        return 0.0            
    return 0.6745 * (value - baseline.ewma) / baseline.mad()

def score_domain(app_id: str, domain: str, window_dict: dict, hour: int, store) -> tuple[float, bool]:
    metrics = DOMAIN_METRICS[domain]
    zs = []
    for m in metrics:
        if m not in window_dict:
            continue
        baseline = store.get(app_id, m, "ALL")
        z = robust_z(window_dict[m], baseline)
        zs.append(abs(z))
    worst = max(zs) if zs else 0.0
    return worst, worst >= DEVIATION_Z_THRESHOLD


class DebounceTracker:
    """Requires N consecutive deviated windows per domain before calling it real —
    stops a single noisy spike from firing a trigger."""
    def __init__(self):
        self._streaks: dict[tuple[str, str], int] = {}

    def observe(self, app_id: str, domain: str, deviated: bool) -> bool:
        key = (app_id, domain)
        self._streaks[key] = self._streaks.get(key, 0) + 1 if deviated else 0
        return self._streaks[key] >= DEBOUNCE_WINDOWS