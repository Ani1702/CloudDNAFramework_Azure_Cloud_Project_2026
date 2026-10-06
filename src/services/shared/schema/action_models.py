from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from .decision_models import TriggerEvent


class CandidateGenome(BaseModel):
    replica_count: int = 2
    cpu_cores: float = 1.0
    memory_gb: float = 1.0
    node_thread_pool_size: int = 20
    redis_cache_enabled: bool = False
    redis_cache_ttl_seconds: int = 0
    db_connection_pool_max: Optional[int] = 10
    rate_limiting_enabled: bool = False
    rate_limit_rps: Optional[int] = 100
    health_check_timeout_sec: int = 2


class CandidateEvaluationResult(BaseModel):
    trigger_id: str
    candidate_id: str
    genome: Dict[str, Any]
    generation_method: str = "bayesian_optimizer"  # bayesian_optimizer | historical_seed | mutation | fast_path_reuse
    readiness_ts: str
    raw_metrics: Dict[str, Any]
    security_probe_results: Dict[str, Any] = Field(default_factory=dict)


class CandidateScoreRecord(BaseModel):
    candidate_id: str
    genome: Dict[str, Any]
    generation_method: str
    domain_scores: Dict[str, float]  # {performance, cost, security, scaling, recovery}
    composite_score: float
    raw_metrics: Dict[str, Any] = Field(default_factory=dict)


class PromotionRecord(BaseModel):
    trigger_id: str
    winning_candidate_id: str
    winning_genome: Dict[str, Any] = Field(default_factory=dict)
    domain_scores: Dict[str, float]
    composite_score: float
    runner_up_score: float
    confidence_margin: float
    gate_passed: bool
    promoted_at: str
    rollback_config: Dict[str, Any]
    explanation: str
    explanation_source: str = "nemotron_ultra"


class ActionBatchRequest(BaseModel):
    trigger_event: TriggerEvent
    candidates: List[CandidateEvaluationResult]