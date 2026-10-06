from typing import Dict, Any, Optional
import logging
from datetime import datetime

from ..llm.nemotron_client import NemotronLLMClient

logger = logging.getLogger("clouddna.promotion")


class PromotionEngine:
    def __init__(self, cosmos_client: Optional[Any] = None):
        self.genome_container = None
        self.events_container = None
        self.llm_client = NemotronLLMClient()

        if cosmos_client:
            try:
                db = cosmos_client.get_database_client("clouddna")
                self.genome_container = db.get_container_client("genome_library")
                self.events_container = db.get_container_client("trigger_events")
            except Exception as e:
                logger.warning("Could not initialize Cosmos DB containers in PromotionEngine: %s", e)

    def promote(
        self,
        trigger_event: Dict[str, Any],
        winner: Dict[str, Any],
        runner_up: Optional[Dict[str, Any]],
        confidence_margin: float,
        gate_passed: bool,
    ) -> Dict[str, Any]:
        app_id = trigger_event.get("app_id", "medusa")
        trigger_id = trigger_event.get("trigger_id", "trig-unknown")
        category = trigger_event.get("trigger_category", "TRAFFIC_SPIKE")
        rollback_config = trigger_event.get("current_production_config", {})

        winning_genome = winner.get("genome", {})
        promoted_ts = datetime.utcnow().isoformat() + "Z"

        explanation = self.llm_client.generate_explanation(winner, runner_up, category)

        promotion_record = {
            "id": f"prom-{trigger_id}",
            "app_id": app_id,
            "trigger_id": trigger_id,
            "winning_candidate_id": winner.get("candidate_id"),
            "winning_genome": winning_genome,
            "domain_scores": winner.get("domain_scores", {}),
            "composite_score": winner.get("composite_score", 0.0),
            "runner_up_score": runner_up.get("composite_score", 0.0) if runner_up else 0.0,
            "confidence_margin": confidence_margin,
            "gate_passed": gate_passed,
            "promoted_at": promoted_ts,
            "rollback_config": rollback_config,
            "explanation": explanation,
            "explanation_source": "nemotron_ultra",
        }

        # Archive verified genome for future Fast-Path reuse
        if gate_passed:
            genome_library_entry = {
                "id": f"genome-{trigger_id}",
                "app_id": app_id,
                "trigger_category": category,
                "fingerprint_vector": trigger_event.get("fingerprint_vector", []),
                "genome": winning_genome,
                "verified_composite_score": winner.get("composite_score", 0.0),
                "promoted_at": promoted_ts,
            }

            if self.genome_container:
                try:
                    self.genome_container.upsert_item(genome_library_entry)
                    logger.info("Archived winning genome %s in genome_library.", genome_library_entry["id"])
                except Exception as e:
                    logger.error("Failed to archive genome in Cosmos DB: %s", e)

        if self.events_container:
            try:
                self.events_container.upsert_item({
                    **trigger_event,
                    "id": trigger_id,
                    "resolution_status": "PROMOTED" if gate_passed else "GATE_REJECTED",
                    "promoted_record_id": promotion_record["id"],
                })
            except Exception as e:
                logger.error("Failed to update trigger_event resolution status: %s", e)

        logger.info("Successfully executed promotion for trigger %s. Winner: %s", trigger_id, winner.get("candidate_id"))
        return promotion_record