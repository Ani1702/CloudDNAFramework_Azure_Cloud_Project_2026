import numpy as np

def classify(confirmed_domains: set[str]) -> str:
    if "security" in confirmed_domains:
        return "SECURITY_ANOMALY"
    if "recovery" in confirmed_domains:
        return "FAILURE_RECOVERY"
    if "performance" in confirmed_domains and "scaling" in confirmed_domains:
        return "TRAFFIC_SPIKE"
    if "performance" in confirmed_domains:
        return "PERFORMANCE_DEGRADATION"
    return "RESOURCE_SATURATION"

def severity_from_zscores(domain_scores: dict[str, float]) -> str:
    worst = max(domain_scores.values(), default=0)
    if worst >= 8:
        return "HIGH"
    if worst >= 5:
        return "MEDIUM"
    return "LOW"

def forecast_throughput(recent_windows: list[dict], eval_window_sec: int = 90) -> dict:
    """Linear extrapolation of throughput_rps over the last few windows, projected
    forward to roughly when a sandbox evaluation would finish — this is what answers
    the 'trigger at 10k rps, actually hits 20k by the time you're done' problem."""
    ts = np.arange(len(recent_windows))
    rps = np.array([w["performance"]["throughput_rps"] for w in recent_windows])
    if len(rps) < 2:
        return {"throughput_rps_at_eval_end": float(rps[-1]) if len(rps) else 0.0,
                "method": "insufficient_data"}
    slope, intercept = np.polyfit(ts, rps, 1)
    # TODO: read window_sec from the window objects instead of assuming 10
    steps_ahead = eval_window_sec / 10   # assuming 10s windows
    projected = slope * (ts[-1] + steps_ahead) + intercept
    return {"throughput_rps_at_eval_end": float(max(projected, 0)),
            "method": "linear_extrapolation"}