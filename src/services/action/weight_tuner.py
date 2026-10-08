from typing import Dict, Any, Optional
import logging
from datetime import datetime

logger = logging.getLogger("clouddna.weight_tuner")


def adjust_weights(
    current_weights: Dict[str, float],
    predicted_scores: Dict[str, float],
    actual_post_promotion_scores: Dict[str, float],
    learning_rate: float = 0.05,
) -> Dict[str, float]:
    """
    If a domain's actual outcome matched its predicted score well, nudge its weight up slightly.
    If off, nudge down.
    """
    new_weights: Dict[str, float] = {}
    for domain, w in current_weights.items():
        pred = predicted_scores.get(domain, 0.5)
        actual = actual_post_promotion_scores.get(domain, pred)
        error = abs(pred - actual)
        accuracy = 1.0 - min(error, 1.0)

        adjusted = w + learning_rate * (accuracy - 0.5) * w
        new_weights[domain] = max(0.05, adjusted)

    total = sum(new_weights.values()) or 1.0
    return {d: round(w / total, 3) for d, w in new_weights.items()}


class WeightTunerService:
    def __init__(self, cosmos_client: Optional[Any] = None):
        self.weights_container = None
        if cosmos_client:
            try:
                self.weights_container = cosmos_client.get_database_client("clouddna").get_container_client("fitness_weights")
            except Exception as e:
                logger.warning("Could not initialize fitness_weights container: %s", e)

    def tune_and_persist(
        self,
        app_id: str,
        trigger_category: str,
        current_weights: Dict[str, float],
        predicted_scores: Dict[str, float],
        actual_scores: Dict[str, float],
    ) -> Dict[str, float]:
        updated_weights = adjust_weights(current_weights, predicted_scores, actual_scores)
        cache_key = f"{app_id}_{trigger_category}"

        record = {
            "id": cache_key,
            "app_id": app_id,
            "trigger_category": trigger_category,
            "weights": updated_weights,
            "prior_weights": current_weights,
            "last_tuned_at": datetime.utcnow().isoformat() + "Z",
        }

        if self.weights_container:
            try:
                self.weights_container.upsert_item(record)
                logger.info("Persisted updated fitness weights for %s to Cosmos DB: %s", cache_key, updated_weights)
            except Exception as e:
                logger.error("Failed to persist updated fitness weights: %s", e)

        return updated_weights