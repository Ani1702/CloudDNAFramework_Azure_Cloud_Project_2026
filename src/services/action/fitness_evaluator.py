from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger("clouddna.fitness")

DEFAULT_CATEGORY_WEIGHTS: Dict[str, Dict[str, float]] = {
    "TRAFFIC_SPIKE": {
        "performance": 0.50,
        "cost": 0.15,
        "security": 0.10,
        "scaling": 0.20,
        "recovery": 0.05,
    },
    "PERFORMANCE_DEGRADATION": {
        "performance": 0.55,
        "cost": 0.10,
        "security": 0.10,
        "scaling": 0.15,
        "recovery": 0.10,
    },
    "RESOURCE_SATURATION": {
        "performance": 0.25,
        "cost": 0.35,
        "security": 0.10,
        "scaling": 0.20,
        "recovery": 0.10,
    },
    "SECURITY_ANOMALY": {
        "performance": 0.10,
        "cost": 0.05,
        "security": 0.70,
        "scaling": 0.10,
        "recovery": 0.05,
    },
    "FAILURE_RECOVERY": {
        "performance": 0.10,
        "cost": 0.05,
        "security": 0.05,
        "scaling": 0.15,
        "recovery": 0.65,
    },
}


class FitnessEvaluator:
    def __init__(self, cosmos_client: Optional[Any] = None):
        self.weights_container = None
        self._weights_cache: Dict[str, Dict[str, float]] = {}

        if cosmos_client:
            try:
                self.weights_container = cosmos_client.get_database_client("clouddna").get_container_client("fitness_weights")
            except Exception as e:
                logger.warning("Could not initialize fitness_weights Cosmos DB container: %s", e)

    def get_weights_for_category(self, app_id: str, trigger_category: str) -> Dict[str, float]:
        cache_key = f"{app_id}_{trigger_category}"
        if cache_key in self._weights_cache:
            return self._weights_cache[cache_key]

        if self.weights_container:
            try:
                item = self.weights_container.read_item(item=cache_key, partition_key=app_id)
                weights = item.get("weights")
                if weights:
                    self._weights_cache[cache_key] = weights
                    return weights
            except Exception:
                pass

        default_w = DEFAULT_CATEGORY_WEIGHTS.get(trigger_category, DEFAULT_CATEGORY_WEIGHTS["TRAFFIC_SPIKE"])
        return dict(default_w)

    def score_performance(self, raw_metrics: Dict[str, Any]) -> float:
        p95 = float(raw_metrics.get("p95_latency_ms", 300.0))
        err = float(raw_metrics.get("error_rate_pct", 0.0))
        rps = float(raw_metrics.get("throughput_rps", 200.0))

        s_lat = max(0.0, min(1.0, 1.0 - ((p95 - 100.0) / 1000.0)))
        s_err = max(0.0, min(1.0, 1.0 - (err / 5.0)))
        s_rps = min(1.0, max(0.2, rps / 500.0))

        return round(0.50 * s_lat + 0.35 * s_err + 0.15 * s_rps, 3)

    def score_cost(self, raw_metrics: Dict[str, Any], genome: Dict[str, Any]) -> float:
        hourly_cost = raw_metrics.get("hourly_compute_cost_usd")
        if hourly_cost is None:
            replicas = float(genome.get("replica_count", 2))
            cpu = float(genome.get("cpu_cores", 1.0))
            hourly_cost = replicas * (cpu * 0.045)

        hourly_cost = float(hourly_cost)
        cost_score = max(0.0, min(1.0, 1.0 - (hourly_cost / 0.50)))
        return round(cost_score, 3)

    def score_security(self, security_probes: Dict[str, Any]) -> float:
        if not security_probes:
            return 1.0

        blocked = 1.0 if security_probes.get("auth_bruteforce_blocked", True) else 0.0
        malformed_5xx = float(security_probes.get("malformed_req_5xx_rate", 0.0))
        resilience = max(0.0, min(1.0, 1.0 - malformed_5xx))

        return round(0.60 * blocked + 0.40 * resilience, 3)

    def score_scaling(self, raw_metrics: Dict[str, Any]) -> float:
        cold_start_ms = float(raw_metrics.get("cold_start_time_ms", 0.0))
        s_scale = max(0.0, min(1.0, 1.0 - (cold_start_ms / 500.0)))
        return round(s_scale, 3)

    def score_recovery(self, raw_metrics: Dict[str, Any]) -> float:
        restarts = float(raw_metrics.get("restart_count", 0.0))
        health_fail = float(raw_metrics.get("health_check_failure_rate_pct", 0.0))

        s_restart = max(0.0, min(1.0, 1.0 - (restarts / 5.0)))
        s_health = max(0.0, min(1.0, 1.0 - (health_fail / 50.0)))

        return round(0.50 * s_restart + 0.50 * s_health, 3)

    def evaluate_candidate(
        self,
        candidate: Dict[str, Any],
        trigger_category: str,
        weights: Dict[str, float],
    ) -> Dict[str, Any]:
        raw = candidate.get("raw_metrics", {})
        genome = candidate.get("genome", {})
        sec_probes = candidate.get("security_probe_results", {})

        s_perf = self.score_performance(raw)
        s_cost = self.score_cost(raw, genome)
        s_sec = self.score_security(sec_probes)
        s_scale = self.score_scaling(raw)
        s_rec = self.score_recovery(raw)

        domain_scores = {
            "performance": s_perf,
            "cost": s_cost,
            "security": s_sec,
            "scaling": s_scale,
            "recovery": s_rec,
        }

        composite = sum(domain_scores[d] * weights.get(d, 0.20) for d in domain_scores)
        composite = max(0.0, min(1.0, composite))

        return {
            "candidate_id": candidate["candidate_id"],
            "genome": genome,
            "generation_method": candidate.get("generation_method", "bayesian_optimizer"),
            "domain_scores": domain_scores,
            "composite_score": round(composite, 3),
            "raw_metrics": raw,
        }

    def rank_candidates(
        self,
        candidates: List[Dict[str, Any]],
        app_id: str,
        trigger_category: str,
    ) -> List[Dict[str, Any]]:
        weights = self.get_weights_for_category(app_id, trigger_category)
        scored = [
            self.evaluate_candidate(c, trigger_category, weights)
            for c in candidates
        ]
        scored.sort(key=lambda x: x["composite_score"], reverse=True)
        return scored