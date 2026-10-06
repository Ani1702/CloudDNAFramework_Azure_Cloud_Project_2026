import os
import json
import logging
from typing import Dict, Any

try:
    import azure.functions as func
except ImportError:
    func = None

from .candidate_generator import CandidateGenerator
from .sandbox_orchestrator import SandboxOrchestrator
from .traffic_dispatcher import TrafficDispatcher
from .evaluation_coordinator import EvaluationCoordinator

logger = logging.getLogger("clouddna.experimentation")
logging.basicConfig(level=logging.INFO)

try:
    from azure.cosmos import CosmosClient
except ImportError:
    CosmosClient = None

cosmos_conn_str = os.getenv("COSMOS_CONN_STR")
cosmos_client = CosmosClient.from_connection_string(cosmos_conn_str) if (cosmos_conn_str and CosmosClient) else None

generator = CandidateGenerator(cosmos_client=cosmos_client)
orchestrator = SandboxOrchestrator()
dispatcher = TrafficDispatcher()
coordinator = EvaluationCoordinator(cosmos_client=cosmos_client)


def run_experimentation_workflow(trigger_event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Executes the full Member 2 Experimentation workflow:
    1. Check fast-path (skip if True)
    2. Generate candidates (Optuna + Nemotron priors)
    3. Deploy to sandbox and align windows (Novelty #1)
    4. Replay lock-step traffic and security probes (Novelties #2 & #4)
    5. Publish candidate evaluation results
    """
    if trigger_event.get("fast_path_match", {}).get("found", False):
        logger.info("Fast-path match detected. Skipping Experimentation.")
        return {"status": "skipped", "reason": "fast_path_match"}

    app_id = trigger_event.get("app_id", "medusa")
    trigger_id = trigger_event.get("trigger_id", "trig-unknown")

    # Fetch top-k historical genomes from Cosmos DB for seeding
    historical_genomes = []
    if cosmos_client:
        try:
            container = cosmos_client.get_database_client("clouddna").get_container_client("genome_library")
            query = "SELECT TOP 3 * FROM c WHERE c.app_id = @app_id ORDER BY c._ts DESC"
            historical_genomes = list(container.query_items(query=query, parameters=[{"name": "@app_id", "value": app_id}], enable_cross_partition_query=False))
        except Exception as e:
            logger.warning("Could not fetch historical genomes: %s", e)

    # 1. Candidate Generation
    candidates = generator.generate_candidates(trigger_event, historical_genomes, num_candidates=3)

    # 2. Sandbox Deployment & Readiness Gating (Novelty #1)
    synced_candidates = orchestrator.deploy_and_synchronize(candidates, app_id=app_id)

    # 3. Traffic Replay & Security Probing (Novelties #2 & #4)
    evaluated_candidates = dispatcher.replay_and_probe(synced_candidates)

    # 4. Teardown Sandbox
    orchestrator.teardown_sandbox(evaluated_candidates)

    # 5. Emit Results to Member 3 Action Layer
    coordinator.publish_candidate_results(evaluated_candidates, app_id=app_id)

    return {
        "status": "completed",
        "trigger_id": trigger_id,
        "candidates_evaluated": len(evaluated_candidates),
        "results": evaluated_candidates,
    }


if func:
    app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

    @app.event_grid_trigger(arg_name="event")
    def on_trigger_event(event: func.EventGridEvent):
        """Listens for CloudDNA.TriggerEvent published by Member 3's Decision Layer."""
        try:
            trigger_payload = event.get_json()
            run_experimentation_workflow(trigger_payload)
        except Exception:
            logger.exception("Failed processing TriggerEvent in Experimentation Layer.")

    @app.route(route="run_experimentation", methods=["POST"])
    def http_run_experimentation(req: func.HttpRequest) -> func.HttpResponse:
        """HTTP endpoint for manual or integration testing."""
        try:
            body = req.get_json()
        except Exception as e:
            return func.HttpResponse(json.dumps({"error": str(e)}), status_code=400, mimetype="application/json")

        result = run_experimentation_workflow(body)
        return func.HttpResponse(json.dumps(result, indent=2), status_code=200, mimetype="application/json")