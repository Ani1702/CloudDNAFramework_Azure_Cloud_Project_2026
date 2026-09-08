from pydantic import BaseModel
from typing import Literal, Optional
from datetime import datetime

class DomainMetrics(BaseModel):
    performance: dict
    cost: dict
    security: dict
    scaling: dict
    recovery: dict

class StreamAWindow(BaseModel):
    app_id: str
    ts: datetime
    window_sec: int
    performance: dict
    cost: dict
    security: dict
    scaling: dict
    recovery: dict

class BaselineRecord(BaseModel):
    app_id: str
    metric: str
    hour_of_day: int
    ewma: float
    mad: float
    sample_count: int

class DeviationRecord(BaseModel):
    app_id: str
    domain: str
    robust_z_score: float
    deviated: bool

TriggerCategory = Literal[
    "TRAFFIC_SPIKE", "PERFORMANCE_DEGRADATION", "RESOURCE_SATURATION",
    "SECURITY_ANOMALY", "FAILURE_RECOVERY"
]

class TriggerEvent(BaseModel):
    app_id: str
    trigger_id: str
    timestamp: datetime
    trigger_category: TriggerCategory
    severity: Literal["LOW", "MEDIUM", "HIGH"]
    forecasted_load: dict
    fingerprint_vector: list[float]
    fingerprint_version: str = "v1"
    current_production_config: dict
    deviated_metrics: dict
    fast_path_match: dict

