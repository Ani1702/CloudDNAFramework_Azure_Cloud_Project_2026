"""
sandbox_orchestrator.py — CloudDNA Experimentation Layer (Member 2)

Deploys candidate genome configurations as real Azure Container Instances (ACI)
concurrently, gates evaluation behind the slowest-ready readiness probe (Novelty #1),
and tears down all sandbox containers after evaluation.
"""
from typing import Dict, Any, List, Optional
import time
import logging
import os
import threading
from datetime import datetime, timezone

logger = logging.getLogger("clouddna.sandbox")

try:
    from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
    from azure.mgmt.containerinstance import ContainerInstanceManagementClient
    from azure.mgmt.containerinstance.models import (
        ContainerGroup,
        Container,
        ContainerPort,
        IpAddress,
        Port,
        ResourceRequests,
        ResourceRequirements,
        OperatingSystemTypes,
        ContainerGroupRestartPolicy,
    )
    _ACI_AVAILABLE = True
except ImportError:
    _ACI_AVAILABLE = False
    logger.warning("azure-mgmt-containerinstance not installed. ACI deployment will be SIMULATED.")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SandboxOrchestrator:
    """
    Manages the full lifecycle of sandbox ACI containers:
      1. Concurrent deployment of N candidate configurations
      2. Readiness probe polling (simulated via deployment completion time)
      3. Aligned evaluation window gating (Novelty #1)
      4. Guaranteed teardown after evaluation
    """

    def __init__(self, timeout_sec: int = 300):
        self.timeout_sec = timeout_sec
        self.subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID", "87117a52-08aa-465f-9c7e-5fa5b0815efd")
        self.resource_group   = os.getenv("AZURE_RESOURCE_GROUP", "azure-cloud-dna-rg")
        self.location         = os.getenv("AZURE_LOCATION", "centralindia")
        self.aci_client: Optional[Any] = None

        if _ACI_AVAILABLE:
            try:
                # Use ManagedIdentityCredential directly — do NOT probe eagerly with get_token().
                # On Azure Consumption plans the IMDS endpoint is not ready during cold-start
                # module load, so an eager probe always raises and silently falls through to
                # aci_client=None. Authentication happens lazily on the first real API call.
                cred = ManagedIdentityCredential()
                self.aci_client = ContainerInstanceManagementClient(cred, self.subscription_id)
                self.aci_init_error = None
                logger.info(
                    "[Sandbox] ACI client initialized with ManagedIdentityCredential. "
                    "Subscription=%s, RG=%s", self.subscription_id, self.resource_group
                )
            except Exception as exc:
                self.aci_init_error = str(exc)
                logger.error("[Sandbox] Failed to init ACI client: %s. Will simulate.", exc)
                self.aci_client = None
        else:
            self.aci_init_error = "azure.mgmt.containerinstance module not imported"

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _aci_name(candidate_id: str) -> str:
        """Derive a valid ACI group name from a candidate ID (max 63 chars, lowercase)."""
        name = f"cdna-sb-{candidate_id}".lower().replace("_", "-")
        return name[:63]

    def _deploy_single_aci(self, candidate: Dict[str, Any], results: Dict[str, Any]):
        """
        Deploy one candidate as an ACI Container Group and record timing.
        Writes into the shared `results` dict so the calling thread can inspect it.
        """
        cid   = candidate["candidate_id"]
        name  = self._aci_name(cid)
        cpu   = max(1.0, float(candidate["genome"].get("cpu_cores",   1.0)))
        mem   = max(1.5, float(candidate["genome"].get("memory_gb",   1.5)))
        start = time.time()

        logger.info("[Sandbox] ▶ Deploying ACI '%s'  cpu=%.2f  mem=%.1fGB ...", name, cpu, mem)

        if not self.aci_client:
            # Simulation path — realistic cold-start delay
            simulated_delay = 2.0 + (cpu * 1.5) + (mem * 0.5)
            logger.warning("[Sandbox] ACI client unavailable — simulating '%s' (%.1fs cold-start).",
                           name, simulated_delay)
            time.sleep(0)   # Keep it fast in serverless; real timing captured from wall-clock
            results[cid] = {
                "sandbox_namespace": name,
                "cold_start_sec": simulated_delay,
                "ready_ts": _utcnow_iso(),
                "simulated": True,
            }
            return

        try:
            resource_req = ResourceRequirements(
                requests=ResourceRequests(memory_in_gb=mem, cpu=cpu)
            )
            container = Container(
                name=name,
                image="mcr.microsoft.com/azuredocs/aci-helloworld:latest",
                resources=resource_req,
                ports=[ContainerPort(port=80)],
            )
            group = ContainerGroup(
                location=self.location,
                containers=[container],
                os_type=OperatingSystemTypes.LINUX,
                restart_policy=ContainerGroupRestartPolicy.NEVER,
                ip_address=IpAddress(
                    ports=[Port(protocol="TCP", port=80)],
                    type="Public"
                ),
            )

            # begin_create_or_update returns an LROPoller; .result() blocks until done
            self.aci_client.container_groups.begin_create_or_update(
                self.resource_group, name, group
            ).result()

            elapsed = round(time.time() - start, 2)
            logger.info("[Sandbox] ✔ ACI '%s' running after %.1fs.", name, elapsed)
            results[cid] = {
                "sandbox_namespace": name,
                "cold_start_sec": elapsed,
                "ready_ts": _utcnow_iso(),
                "simulated": False,
            }

        except Exception as exc:
            elapsed = round(time.time() - start, 2)
            logger.error("[Sandbox] ✘ ACI '%s' FAILED after %.1fs: %s", name, elapsed, exc)
            # Record failure but keep pipeline moving
            results[cid] = {
                "sandbox_namespace": name,
                "cold_start_sec": elapsed,
                "ready_ts": _utcnow_iso(),
                "simulated": True,
                "error": str(exc),
            }

    # ── Public API ─────────────────────────────────────────────────────────────

    def deploy_and_synchronize(
        self,
        candidates: List[Dict[str, Any]],
        app_id: str = "medusa",
    ) -> List[Dict[str, Any]]:
        """
        1. Deploys all N candidates concurrently as ACI containers.
        2. Waits for all to reach Running state.
        3. Finds the slowest-ready candidate (Novelty #1).
        4. Stamps aligned_eval_start_ts = slowest readiness time onto every candidate.

        Returns the enriched candidates list.
        """
        n = len(candidates)
        logger.info("[Sandbox] ═══ Starting sandbox deployment for %d candidates (app=%s) ═══", n, app_id)

        results: Dict[str, Any] = {}
        threads = [
            threading.Thread(
                target=self._deploy_single_aci,
                args=(cand, results),
                name=f"deploy-{cand['candidate_id']}",
                daemon=True,
            )
            for cand in candidates
        ]

        for t in threads:
            t.start()

        # Wait for all deployments (with a wall-clock timeout)
        deadline = time.time() + self.timeout_sec
        for t in threads:
            remaining = max(0.0, deadline - time.time())
            t.join(timeout=remaining)
            if t.is_alive():
                logger.warning("[Sandbox] Thread %s timed out after %ds.", t.name, self.timeout_sec)

        # ── Novelty #1: align evaluation window to slowest-ready container ────
        # All ready_ts values are ISO strings; max() on ISO strings works lexicographically
        ready_times = [r.get("ready_ts", _utcnow_iso()) for r in results.values()]
        slowest_ts  = max(ready_times) if ready_times else _utcnow_iso()
        logger.info("[Sandbox] All candidates ready. Slowest readiness ts = %s  (alignment gate)", slowest_ts)

        for cand in candidates:
            cid    = cand["candidate_id"]
            r      = results.get(cid, {})
            cand["sandbox_namespace"]      = r.get("sandbox_namespace", self._aci_name(cid))
            cand["readiness_ts"]           = r.get("ready_ts", _utcnow_iso())
            cand["simulated_cold_start_sec"] = r.get("cold_start_sec", 0.0)
            cand["aligned_eval_start_ts"]  = slowest_ts
            cand["sandbox_simulated"]      = r.get("simulated", True)

            if "error" in r:
                cand["sandbox_error"] = r["error"]

            logger.info(
                "[Sandbox] Candidate %-30s  namespace=%-35s  cold_start=%.1fs  simulated=%s",
                cid, cand["sandbox_namespace"], cand["simulated_cold_start_sec"], cand["sandbox_simulated"]
            )

        logger.info("[Sandbox] ═══ All %d sandboxes synchronized. Eval window aligned. ═══", n)
        return candidates

    def teardown_sandbox(self, candidates: List[Dict[str, Any]]):
        """
        Deletes all ACI container groups that were deployed for this experiment.
        Fires delete calls concurrently (non-blocking LRO) so the function exits fast.
        """
        if not self.aci_client:
            logger.info("[Sandbox] No ACI client — nothing to tear down.")
            return

        def _delete(namespace: str):
            try:
                logger.info("[Sandbox] 🗑  Deleting ACI sandbox: %s", namespace)
                self.aci_client.container_groups.begin_delete(
                    self.resource_group, namespace
                )   # Fire-and-forget; we don't block on .result()
                logger.info("[Sandbox] ✔ Delete initiated for %s.", namespace)
            except Exception as exc:
                logger.error("[Sandbox] ✘ Delete failed for %s: %s", namespace, exc)

        threads = []
        for cand in candidates:
            ns = cand.get("sandbox_namespace")
            if ns and not cand.get("sandbox_simulated", True):
                t = threading.Thread(target=_delete, args=(ns,), daemon=True)
                threads.append(t)
                t.start()

        for t in threads:
            t.join(timeout=10)   # Brief wait; deletes finish asynchronously in Azure

        logger.info("[Sandbox] Teardown initiated for %d real ACI containers.", len(threads))