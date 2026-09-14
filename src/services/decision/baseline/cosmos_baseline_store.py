# pyrefly: ignore [missing-import]
from azure.cosmos import CosmosClient
import os, json
from .ewma_mad import MetricBaseline

class CosmosBaselineStore:
    def __init__(self):
        client = CosmosClient.from_connection_string(os.environ["COSMOS_CONN_STR"])
        self.container = client.get_database_client("clouddna").get_container_client("baselines")

    def _id(self, app_id, metric, hour): return f"{app_id}:{metric}:{hour}"

    def _load(self, app_id, metric, hour) -> MetricBaseline:
        try:
            item = self.container.read_item(self._id(app_id, metric, hour), partition_key=app_id)
            mb = MetricBaseline(ewma=item["ewma"])
            mb.recent_values.extend(item["recent_values"])
            return mb
        except Exception:
            return MetricBaseline()

    def update(self, app_id, metric, hour_of_day, value):
        # 1) per-hour bucket — exists so the schema genuinely has hour-of-day
        #    data in it, for the report/demo
        self._update_bucket(app_id, metric, hour_of_day, value)

        # 2) pooled bucket — this is what deviation scoring reads from.
        #    Fed sequentially, every window, so EWMA's recency-weighting
        #    is still meaningful here (unlike if we tried to merge the
        #    24 buckets after the fact, which would scramble time order).
        self._update_bucket(app_id, metric, "ALL", value)

    def _update_bucket(self, app_id, metric, hour_of_day, value):
        mb = self._load(app_id, metric, hour_of_day)
        mb.update(value)
        self.container.upsert_item({
            "id": self._id(app_id, metric, hour_of_day), "app_id": app_id,
            "metric": metric, "hour_of_day": hour_of_day,
            "ewma": mb.ewma, "recent_values": list(mb.recent_values),
        })

    def get(self, app_id, metric, hour_of_day) -> MetricBaseline | None:
        mb = self._load(app_id, metric, hour_of_day)
        return mb if mb.ewma is not None else None