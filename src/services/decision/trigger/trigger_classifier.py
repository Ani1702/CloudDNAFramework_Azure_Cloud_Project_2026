from typing import Dict, Any, List, Set
# pyrefly: ignore [missing-import]
from models.decision_models import TriggerCategory, SeverityLevel


def classify(confirmed_domains: Set[str], window: Dict[str, Any]) -> TriggerCategory:
    if "security" in confirmed_domains:
        return TriggerCategory.SECURITY_ANOMALY

    if "recovery" in confirmed_domains:
        return TriggerCategory.FAILURE_RECOVERY

    perf = window.get("performance", {})
    cpu = float(perf.get("cpu_utilization_pct", 0.0))
    mem = float(perf.get("memory_utilization_pct", 0.0))
    rps = float(perf.get("throughput_rps", 0.0))
    p95 = float(perf.get("p95_latency_ms", 0.0))
    err = float(perf.get("error_rate_pct", 0.0))

    if "performance" in confirmed_domains or "scaling" in confirmed_domains:
        if rps >= 300.0 or (rps >= 200.0 and cpu >= 60.0):
            return TriggerCategory.TRAFFIC_SPIKE
        if (cpu >= 80.0 or mem >= 80.0) and rps < 200.0:
            return TriggerCategory.RESOURCE_SATURATION
        if p95 >= 350.0 or err >= 2.0:
            return TriggerCategory.PERFORMANCE_DEGRADATION

    if "cost" in confirmed_domains:
        return TriggerCategory.RESOURCE_SATURATION

    return TriggerCategory.TRAFFIC_SPIKE


def severity_from_zscores(domain_scores: Dict[str, float]) -> SeverityLevel:
    max_z = max(domain_scores.values()) if domain_scores else 0.0
    if max_z >= 5.0:
        return SeverityLevel.HIGH
    elif max_z >= 3.0:
        return SeverityLevel.HIGH
    elif max_z >= 1.5:
        return SeverityLevel.MEDIUM
    else:
        return SeverityLevel.LOW


def forecast_throughput(recent_windows: List[Dict[str, Any]], horizon_sec: int = 90) -> Dict[str, Any]:
    if not recent_windows:
        return {"throughput_rps_at_eval_end": 100.0, "method": "linear_extrapolation"}

    rps_series = [
        float(w.get("performance", {}).get("throughput_rps", 0.0))
        for w in recent_windows
    ]

    latest_rps = rps_series[-1]
    if len(rps_series) < 2:
        projected = latest_rps * 1.20
    else:
        delta = (rps_series[-1] - rps_series[0]) / max(len(rps_series) - 1, 1)
        steps = max(int(horizon_sec / 10), 1)
        projected = latest_rps + (delta * steps)

    projected = max(projected, latest_rps, 10.0)
    return {
        "throughput_rps_at_eval_end": round(projected, 1),
        "method": "linear_extrapolation",
    }