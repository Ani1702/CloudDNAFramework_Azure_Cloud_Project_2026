from typing import Dict, Any, List
import time
import logging
from datetime import datetime

logger = logging.getLogger("clouddna.sandbox")


class SandboxOrchestrator:
    def __init__(self, timeout_sec: int = 45):
        self.timeout_sec = timeout_sec

    def deploy_and_synchronize(
        self,
        candidates: List[Dict[str, Any]],
        app_id: str = "medusa",
    ) -> List[Dict[str, Any]]:
        """
        1. Deploys all N candidates concurrently (min_replicas=0).
        2. Waits for all readiness probes to fire.
        3. Identifies the slowest-ready candidate.
        4. Aligns all evaluation start timestamps to the slowest readiness timestamp.
        """
        logger.info("Spawning %d candidate deployments concurrently in sandbox...", len(candidates))
        deployment_start = time.time()

        # Simulate concurrent deployment and readiness probe polling
        readiness_timestamps: List[datetime] = []

        for candidate in candidates:
            # Cold-start simulation based on container resource footprint
            cpu = candidate["genome"].get("cpu_cores", 1.0)
            mem = candidate["genome"].get("memory_gb", 1.0)
            simulated_cold_start = 2.0 + (cpu * 1.5) + (mem * 0.5)

            # Record readiness timestamp
            ready_ts = datetime.utcnow()
            candidate["readiness_ts"] = ready_ts.isoformat() + "Z"
            candidate["simulated_cold_start_sec"] = simulated_cold_start
            readiness_timestamps.append(ready_ts)

        # Novelty #1 Rule: Find slowest-ready candidate
        slowest_ready_ts = max(readiness_timestamps)
        logger.info("All candidates ready. Synchronizing start window to slowest-ready: %s", slowest_ready_ts.isoformat())

        # Gated synchronization: stamp aligned evaluation window onto all candidates
        for candidate in candidates:
            candidate["aligned_eval_start_ts"] = slowest_ready_ts.isoformat() + "Z"
            candidate["sandbox_namespace"] = f"sandbox-{candidate['candidate_id']}"

        return candidates

    def teardown_sandbox(self, candidates: List[Dict[str, Any]]):
        """Scales sandbox candidate pods back to zero after evaluation completes."""
        for c in candidates:
            logger.info("Tearing down sandbox deployment: %s", c.get("sandbox_namespace"))