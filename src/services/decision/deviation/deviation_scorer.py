from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger("clouddna.deviation")

DOMAIN_METRICS: Dict[str, List[str]] = {
    "performance": [
        "p95_latency_ms",
        "error_rate_pct",
        "throughput_rps",
        "cpu_utilization_pct",
        "memory_utilization_pct",
    ],
    "cost": [
        "hourly_compute_cost_usd",
    ],
    "security": [
        "failed_auth_attempts_per_min",
        "anomalous_ip_request_rate",
    ],
    "scaling": [
        "current_replica_count",
        "cold_start_time_ms",
    ],
    "recovery": [
        "restart_count",
        "health_check_failure_rate_pct",
    ],
}

# Critical safety thresholds (triggers even during cold-start baseline calibration)
CRITICAL_ABS_THRESHOLDS: Dict[str, float] = {
    "error_rate_pct": 5.0,
    "p95_latency_ms": 1000.0,
    "restart_count": 1.0,
    "health_check_failure_rate_pct": 10.0,
    "failed_auth_attempts_per_min": 10.0,
    "anomalous_ip_request_rate": 5.0,
    "cpu_utilization_pct": 85.0,
    "memory_utilization_pct": 85.0,
}


def score_domain(
    app_id: str,
    domain: str,
    domain_data: Dict[str, Any],
    hour: int,
    baseline_store: Any,
    z_threshold: float = 2.5,
) -> Tuple[float, bool]:
    metrics = DOMAIN_METRICS.get(domain, [])
    max_z = 0.0
    deviated = False

    for metric in metrics:
        val = domain_data.get(metric)
        if val is None:
            continue
        try:
            val_float = float(val)
        except (ValueError, TypeError):
            continue

        z = baseline_store.compute_zscore(app_id, metric, hour, val_float)
        if z > max_z:
            max_z = z

        if z >= z_threshold:
            deviated = True

        if metric in CRITICAL_ABS_THRESHOLDS:
            if val_float >= CRITICAL_ABS_THRESHOLDS[metric]:
                deviated = True
                max_z = max(max_z, 3.5)

    return round(max_z, 3), deviated


class DebounceTracker:
    def __init__(self, consecutive_threshold: int = 2):
        self.consecutive_threshold = consecutive_threshold
        self._counts: Dict[str, int] = {}

    def observe(self, app_id: str, domain: str, is_deviated: bool) -> bool:
        key = f"{app_id}_{domain}"
        if is_deviated:
            self._counts[key] = self._counts.get(key, 0) + 1
            if self._counts[key] >= self.consecutive_threshold:
                return True
        else:
            self._counts[key] = max(0, self._counts.get(key, 0) - 1)
        return False

    def reset(self, app_id: str):
        keys_to_del = [k for k in self._counts if k.startswith(f"{app_id}_")]
        for k in keys_to_del:
            self._counts[k] = 0