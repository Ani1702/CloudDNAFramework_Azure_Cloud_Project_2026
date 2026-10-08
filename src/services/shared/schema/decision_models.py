from typing import Dict, Any, List, Optional
from enum import Enum
from pydantic import BaseModel, Field


class TriggerCategory(str, Enum):
    TRAFFIC_SPIKE = "TRAFFIC_SPIKE"
    PERFORMANCE_DEGRADATION = "PERFORMANCE_DEGRADATION"
    RESOURCE_SATURATION = "RESOURCE_SATURATION"
    SECURITY_ANOMALY = "SECURITY_ANOMALY"
    FAILURE_RECOVERY = "FAILURE_RECOVERY"


class SeverityLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class PerformanceMetrics(BaseModel):
    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    throughput_rps: float = 0.0
    error_rate_pct: float = 0.0
    cpu_utilization_pct: float = 0.0
    memory_utilization_pct: float = 0.0
    queue_depth: float = 0.0


class CostMetrics(BaseModel):
    hourly_compute_cost_usd: float = 0.0


class SecurityMetrics(BaseModel):
    failed_auth_attempts_per_min: float = 0.0
    anomalous_ip_request_rate: float = 0.0


class ScalingMetrics(BaseModel):
    current_replica_count: int = 1
    cold_start_time_ms: float = 0.0


class RecoveryMetrics(BaseModel):
    restart_count: int = 0
    health_check_failure_rate_pct: float = 0.0


class StreamAWindow(BaseModel):
    """
    Stream A: Emitted every 10s by Observation Layer (Member 1) to Decision Layer (Member 3).
    """
    app_id: str
    ts: str
    window_sec: int = 10
    performance: PerformanceMetrics
    cost: CostMetrics
    security: SecurityMetrics
    scaling: ScalingMetrics
    recovery: RecoveryMetrics
    current_production_config: Optional[Dict[str, Any]] = None


class ForecastedLoad(BaseModel):
    throughput_rps_at_eval_end: float
    method: str = "linear_extrapolation"


class FastPathMatch(BaseModel):
    found: bool = False
    matched_genome_id: Optional[str] = None
    similarity: float = 0.0
    genome: Optional[Dict[str, Any]] = None


class TriggerEvent(BaseModel):
    """
    Finalized output of the Decision Layer.
    Emitted to Event Grid for Experimentation Layer (Member 2),
    or routed directly to Promotion Engine if fast_path_match.found is True.
    """
    app_id: str
    trigger_id: str
    timestamp: str
    trigger_category: TriggerCategory
    severity: SeverityLevel
    forecasted_load: ForecastedLoad
    fingerprint_vector: List[float] = Field(..., description="5D vector: [perf, cost, sec, scale, rec]")
    fingerprint_version: str = "v1"
    current_production_config: Dict[str, Any]
    deviated_metrics: Dict[str, Any]
    fast_path_match: FastPathMatch
    expected_candidate_count: Optional[int] = 3