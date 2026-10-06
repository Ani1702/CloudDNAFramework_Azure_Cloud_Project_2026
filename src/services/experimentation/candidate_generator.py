from typing import Dict, Any, List, Optional
import os
import logging
import optuna

try:
    from ..llm.nemotron_client import NemotronLLMClient
except ImportError:
    NemotronLLMClient = None

logger = logging.getLogger("clouddna.candidate_generator")
optuna.logging.set_verbosity(optuna.logging.WARNING)

# Tunable parameter bounds derived strictly from cdna.yaml
LOCI_BOUNDS: Dict[str, Dict[str, Any]] = {
    "replica_count": {"type": "int", "min": 1, "max": 10, "default": 2},
    "cpu_cores": {"type": "float", "min": 0.25, "max": 2.0, "step": 0.25, "default": 1.0},
    "memory_gb": {"type": "float", "min": 0.5, "max": 4.0, "step": 0.5, "default": 1.0},
    "node_thread_pool_size": {"type": "int", "min": 4, "max": 128, "default": 20},
    "redis_cache_enabled": {"type": "bool", "default": False},
    "redis_cache_ttl_seconds": {"type": "int", "min": 0, "max": 3600, "default": 0},
    "db_connection_pool_max": {"type": "int", "min": 5, "max": 100, "default": 10},
    "rate_limiting_enabled": {"type": "bool", "default": False},
    "rate_limit_rps": {"type": "int", "min": 10, "max": 1000, "default": 100},
    "health_check_timeout_sec": {"type": "int", "min": 1, "max": 10, "default": 2},
}


class CandidateGenerator:
    def __init__(self, cosmos_client: Optional[Any] = None):
        self.llm_client = NemotronLLMClient() if NemotronLLMClient else None
        self.cosmos_client = cosmos_client

    def generate_candidates(
        self,
        trigger_event: Dict[str, Any],
        historical_genomes: List[Dict[str, Any]],
        num_candidates: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        Generates num_candidates distinct configurations:
        - Candidate 1: Seeded from Nemotron prior ranges + Optuna
        - Candidate 2: Historical winner mutation (or Optuna exploration)
        - Candidate 3: Optuna Bayesian exploration across remaining bounds
        """
        trigger_id = trigger_event.get("trigger_id", "trig-unknown")
        app_id = trigger_event.get("app_id", "medusa")

        # 1. Obtain Nemotron prior ranges (via Member 3 shared client)
        nemotron_priors = {}
        if self.llm_client:
            try:
                prior_resp = self.llm_client.get_prior_seed(trigger_event, historical_genomes)
                nemotron_priors = prior_resp.get("suggested_ranges", {})
                logger.info("Retrieved Nemotron prior ranges for %s: %s", trigger_id, nemotron_priors)
            except Exception as e:
                logger.warning("Could not obtain Nemotron prior ranges: %s", e)

        candidates: List[Dict[str, Any]] = []

        # 2. Candidate 1: Historical Seed (if high-quality historical match exists)
        if historical_genomes:
            best_hist = historical_genomes[0].get("genome", {})
            mutated_genome = self._mutate_genome(best_hist)
            candidates.append({
                "trigger_id": trigger_id,
                "candidate_id": f"cand-{trigger_id[-6:]}-01",
                "genome": mutated_genome,
                "generation_method": "historical_seed",
            })

        # 3. Use Optuna to generate remaining candidates guided by priors
        study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=42))

        def objective(trial: optuna.Trial) -> float:
            genome = {}
            for param, meta in LOCI_BOUNDS.items():
                p_type = meta["type"]

                # Apply Nemotron narrowed prior if available
                if param in nemotron_priors and isinstance(nemotron_priors[param], list):
                    low, high = nemotron_priors[param][0], nemotron_priors[param][-1]
                else:
                    low, high = meta.get("min"), meta.get("max")

                if p_type == "int":
                    genome[param] = trial.suggest_int(param, int(low), int(high))
                elif p_type == "float":
                    step = meta.get("step", 0.25)
                    genome[param] = trial.suggest_float(param, float(low), float(high), step=step)
                elif p_type == "bool":
                    genome[param] = trial.suggest_categorical(param, [True, False])

            # Dummy internal objective to guide diverse space sampling
            trial.set_user_attr("genome", genome)
            return 1.0

        study.optimize(objective, n_trials=num_candidates * 2)

        # Extract top distinct trials
        seen_genomes = [c["genome"] for c in candidates]
        idx = len(candidates) + 1

        for trial in study.trials:
            g = trial.user_attrs.get("genome")
            if g and g not in seen_genomes:
                seen_genomes.append(g)
                candidates.append({
                    "trigger_id": trigger_id,
                    "candidate_id": f"cand-{trigger_id[-6:]}-0{idx}",
                    "genome": g,
                    "generation_method": "bayesian_optimizer",
                })
                idx += 1
                if len(candidates) >= num_candidates:
                    break

        return candidates

    def _mutate_genome(self, genome: Dict[str, Any]) -> Dict[str, Any]:
        """Applies a safe ±10% mutation to a historical genome."""
        mutated = dict(genome)
        if "replica_count" in mutated:
            mutated["replica_count"] = min(10, max(1, mutated["replica_count"] + 1))
        if "cpu_cores" in mutated:
            mutated["cpu_cores"] = min(2.0, max(0.5, mutated["cpu_cores"]))
        return mutated