import math

def to_unit_interval(z: float, k: float = 6.0) -> float:
    """Maps a z-score to (0,1); k controls how quickly it saturates toward 1."""
    return 1 / (1 + math.exp(-(z - k) / (k / 3)))

def build_fingerprint(domain_scores: dict[str, float]) -> list[float]:
    # fixed order of fingerprint
    order = ["performance", "cost", "security", "scaling", "recovery"]
    return [round(to_unit_interval(domain_scores.get(d, 0.0)), 2) for d in order]