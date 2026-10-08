"""
Constructs a normalized 5-dimensional fingerprint vector:
[performance, cost, security, scaling, recovery]
"""
from typing import Dict, List

ORDERED_DOMAINS = ["performance", "cost", "security", "scaling", "recovery"]

def build_fingerprint(domain_scores: Dict[str, float], z_saturation: float = 4.0) -> List[float]:
    """
    Normalizes domain robust z-scores into a 5D vector of floats in [0.0, 1.0].
    """
    vector: List[float] = []
    for domain in ORDERED_DOMAINS:
        z = domain_scores.get(domain, 0.0)
        norm_val = min(1.0, max(0.0, z / z_saturation))
        vector.append(round(norm_val, 2))
    return vector