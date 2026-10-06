from typing import Dict, Any, Optional, Tuple
import logging
from datetime import datetime
from .ewma_mad import EWMAMADCalculator

logger = logging.getLogger("clouddna.baseline")


class CosmosBaselineStore:
    def __init__(self, cosmos_client: Optional[Any] = None, database_name: str = "clouddna", container_name: str = "baselines"):
        self.calculator = EWMAMADCalculator()
        self.container = None
        self._memory_cache: Dict[str, Dict[str, Any]] = {}

        if cosmos_client is not None:
            try:
                self.container = cosmos_client.get_database_client(database_name).get_container_client(container_name)
                logger.info("CosmosBaselineStore connected to container: %s", container_name)
            except Exception as e:
                logger.warning("Could not connect to Cosmos DB (%s). Using local in-memory store.", e)
        else:
            logger.info("CosmosBaselineStore running in local in-memory mode.")

    def _get_key(self, app_id: str, metric: str, hour: int) -> str:
        return f"{app_id}_{metric}_{hour}"

    def get_baseline(self, app_id: str, metric: str, hour: int) -> Tuple[float, float, int]:
        key = self._get_key(app_id, metric, hour)
        if key in self._memory_cache:
            item = self._memory_cache[key]
            return item["ewma"], item["mad"], item["sample_count"]

        if self.container:
            try:
                item = self.container.read_item(item=key, partition_key=app_id)
                self._memory_cache[key] = item
                return item.get("ewma", 0.0), item.get("mad", 0.0), item.get("sample_count", 0)
            except Exception:
                pass

        return 0.0, 0.0, 0

    def update(self, app_id: str, metric: str, hour: int, current_val: float) -> Tuple[float, float, int]:
        key = self._get_key(app_id, metric, hour)
        prior_ewma, prior_mad, count = self.get_baseline(app_id, metric, hour)

        new_ewma, new_mad, new_count = self.calculator.update_baseline(
            current_val=current_val,
            prior_ewma=prior_ewma,
            prior_mad=prior_mad,
            sample_count=count,
        )

        record = {
            "id": key,
            "app_id": app_id,
            "metric": metric,
            "hour": hour,
            "ewma": round(new_ewma, 4),
            "mad": round(new_mad, 4),
            "sample_count": new_count,
            "last_updated": datetime.utcnow().isoformat() + "Z",
        }

        self._memory_cache[key] = record

        if self.container:
            try:
                self.container.upsert_item(record)
            except Exception as e:
                logger.error("Failed to upsert baseline into Cosmos DB: %s", e)

        return new_ewma, new_mad, new_count

    def compute_zscore(self, app_id: str, metric: str, hour: int, current_val: float) -> float:
        ewma, mad, count = self.get_baseline(app_id, metric, hour)
        if count < 3:
            return 0.0
        return self.calculator.compute_robust_zscore(current_val, ewma, mad)