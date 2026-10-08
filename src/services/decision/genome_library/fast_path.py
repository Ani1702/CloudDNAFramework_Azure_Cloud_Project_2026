from typing import Dict, Any, List, Optional
import math
import logging

logger = logging.getLogger("clouddna.fast_path")


def cosine_similarity(v1: List[float], v2: List[float]) -> float:
    if not v1 or not v2 or len(v1) != len(v2):
        return 0.0

    dot_product = sum(a * b for a, b in zip(v1, v2))
    norm_v1 = math.sqrt(sum(a * a for a in v1))
    norm_v2 = math.sqrt(sum(b * b for b in v2))

    if norm_v1 == 0.0 or norm_v2 == 0.0:
        return 0.0

    sim = dot_product / (norm_v1 * norm_v2)
    return max(0.0, min(1.0, float(sim)))


def find_fast_path(
    current_fingerprint: List[float],
    historical_genomes: List[Dict[str, Any]],
    similarity_threshold: float = 0.90,
) -> Dict[str, Any]:
    if not historical_genomes or not current_fingerprint:
        return {
            "found": False,
            "matched_genome_id": None,
            "similarity": 0.0,
        }

    best_match_id = None
    best_similarity = 0.0
    best_genome: Optional[Dict[str, Any]] = None

    for entry in historical_genomes:
        hist_vector = entry.get("fingerprint_vector")
        if not hist_vector:
            continue

        sim = cosine_similarity(current_fingerprint, hist_vector)
        if sim > best_similarity:
            best_similarity = sim
            best_match_id = entry.get("id") or entry.get("genome_id")
            best_genome = entry.get("genome") or entry.get("production_config")

    if best_similarity >= similarity_threshold and best_match_id is not None:
        logger.info(
            "Fast-path HIT! Matched genome %s with similarity %.3f (threshold: %.2f)",
            best_match_id, best_similarity, similarity_threshold
        )
        return {
            "found": True,
            "matched_genome_id": best_match_id,
            "similarity": round(best_similarity, 3),
            "genome": best_genome or {},
        }

    return {
        "found": False,
        "matched_genome_id": None,
        "similarity": round(best_similarity, 3) if best_similarity > 0 else 0.0,
    }