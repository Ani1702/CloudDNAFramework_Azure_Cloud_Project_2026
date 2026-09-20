from dataclasses import dataclass, field
from collections import deque

EWMA_ALPHA = 0.15          # how fast the baseline adapts; higher = faster but noisier
MAD_WINDOW = 300           # rolling sample count used to compute MAD per bucket

@dataclass
class MetricBaseline:
    ewma: float | None = None
    recent_values: deque = field(default_factory=lambda: deque(maxlen=MAD_WINDOW))

    def update(self, value: float) -> None:
        self.recent_values.append(value)
        self.ewma = value if self.ewma is None else (
            EWMA_ALPHA * value + (1 - EWMA_ALPHA) * self.ewma
        )

    def mad(self) -> float | None:
        if len(self.recent_values) < 5:
            return None          # signal "not enough data" instead of a zero
        vals = sorted(self.recent_values)
        median = vals[len(vals) // 2]
        deviations = sorted(abs(v - median) for v in vals)
        return deviations[len(deviations) // 2] or 1e-6

    def confidence(self) -> float:
        # crude confidence score: caps out once we've seen enough samples for this bucket
        return min(1.0, len(self.recent_values) / 50)


class BaselineStore:
    """In-memory for local dev; swap the two methods below for Cosmos DB reads/writes
    against the `baselines` container (partition key /app_id) once Member 1's telemetry
    is live."""

    def __init__(self):
        self._store: dict[tuple[str, str, int | str], MetricBaseline] = {}

    def _key(self, app_id: str, metric: str, hour_of_day: int | str):
        return (app_id, metric, hour_of_day)

    def update(self, app_id: str, metric: str, hour_of_day: int, value: float):
        # per-hour bucket — exists so the schema genuinely has hour-of-day data in it
        self._store.setdefault(self._key(app_id, metric, hour_of_day), MetricBaseline()).update(value)
        # pooled bucket — this is what deviation scoring actually reads from
        self._store.setdefault(self._key(app_id, metric, "ALL"), MetricBaseline()).update(value)

    def get(self, app_id: str, metric: str, hour_of_day: int | str) -> MetricBaseline | None:
        return self._store.get(self._key(app_id, metric, hour_of_day))