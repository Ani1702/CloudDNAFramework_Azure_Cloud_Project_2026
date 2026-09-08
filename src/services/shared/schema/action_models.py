from pydantic import BaseModel
from typing import Literal
from datetime import datetime
from .decision_models import TriggerCategory   

class CandidateEvaluationResult(BaseModel):
    trigger_id: str
    candidate_id: str
    genome: dict
    generation_method: str
    readiness_ts: datetime
    raw_metrics: dict
    security_probe_results: dict

class PromotionRecord(BaseModel):
    trigger_id: str
    winning_candidate_id: str
    domain_scores: dict
    composite_score: float
    runner_up_score: float
    confidence_margin: float
    gate_passed: bool
    promoted_at: datetime
    rollback_config: dict
    explanation: str
    explanation_source: str