from typing import Dict, Any, List, Optional
import os
import logging

try:
    from azure.eventgrid import EventGridPublisherClient, EventGridEvent
    from azure.core.credentials import AzureKeyCredential
except ImportError:
    EventGridPublisherClient = None
    EventGridEvent = None
    AzureKeyCredential = None

logger = logging.getLogger("clouddna.coordinator")


class EvaluationCoordinator:
    def __init__(self, cosmos_client: Optional[Any] = None):
        self.cosmos_client = cosmos_client
        self.publisher = None

        endpoint = os.getenv("CANDIDATE_RESULTS_TOPIC_ENDPOINT")
        key = os.getenv("CANDIDATE_RESULTS_TOPIC_KEY")
        if endpoint and key and EventGridPublisherClient and AzureKeyCredential:
            try:
                self.publisher = EventGridPublisherClient(endpoint, AzureKeyCredential(key))
            except Exception as e:
                logger.warning("Could not initialize Event Grid publisher: %s", e)

    def publish_candidate_results(
        self,
        evaluated_candidates: List[Dict[str, Any]],
        app_id: str = "medusa",
    ):
        """
        Emits each candidate result to Cosmos DB and Event Grid.
        """
        for cand in evaluated_candidates:
            record = {
                "id": f"{cand['trigger_id']}_{cand['candidate_id']}",
                "trigger_id": cand["trigger_id"],
                "candidate_id": cand["candidate_id"],
                "genome": cand["genome"],
                "generation_method": cand["generation_method"],
                "readiness_ts": cand["readiness_ts"],
                "raw_metrics": cand["raw_metrics"],
                "security_probe_results": cand["security_probe_results"],
            }

            # 1. Persist to Cosmos DB container 'candidate_evaluations' (Partition Key: /trigger_id)
            if self.cosmos_client:
                try:
                    container = self.cosmos_client.get_database_client("clouddna").get_container_client("candidate_evaluations")
                    container.upsert_item(record)
                    logger.info("Persisted candidate %s to Cosmos DB.", record["id"])
                except Exception as e:
                    logger.error("Failed to persist candidate to Cosmos DB: %s", e)

            # 2. Publish to Event Grid Topic for Member 3's Action Layer
            if self.publisher and EventGridEvent:
                try:
                    self.publisher.send(EventGridEvent(
                        event_type="CloudDNA.CandidateEvaluationResult",
                        data=record,
                        subject=f"candidate/{app_id}",
                        data_version="1.0",
                    ))
                    logger.info("Published candidate %s to Event Grid.", record["id"])
                except Exception as e:
                    logger.error("Failed to publish candidate to Event Grid: %s", e)