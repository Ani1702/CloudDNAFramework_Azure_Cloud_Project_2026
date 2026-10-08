from typing import Tuple
import math


class EWMAMADCalculator:
    def __init__(self, alpha: float = 0.15, beta: float = 0.15, min_mad: float = 1e-4):
        self.alpha = alpha
        self.beta = beta
        self.min_mad = min_mad

    def update_baseline(
        self,
        current_val: float,
        prior_ewma: float,
        prior_mad: float,
        sample_count: int,
    ) -> Tuple[float, float, int]:
        """
        Updates EWMA and MAD with incoming observation.
        """
        if sample_count <= 0 or math.isnan(prior_ewma):
            return float(current_val), max(float(abs(current_val) * 0.1), self.min_mad), 1

        new_ewma = (self.alpha * current_val) + ((1.0 - self.alpha) * prior_ewma)
        current_dev = abs(current_val - new_ewma)
        new_mad = (self.beta * current_dev) + ((1.0 - self.beta) * prior_mad)
        new_mad = max(new_mad, self.min_mad)

        return float(new_ewma), float(new_mad), sample_count + 1

    def compute_robust_zscore(self, current_val: float, ewma: float, mad: float) -> float:
        """
        Computes robust z-score: |x - EWMA| / (1.4826 * MAD).
        """
        effective_mad = max(mad, self.min_mad)
        scale = 1.4826 * effective_mad
        z = abs(current_val - ewma) / scale
        return float(round(z, 4))