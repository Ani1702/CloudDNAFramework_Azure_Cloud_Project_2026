def gate(sorted_candidates: list[dict], min_margin: float) -> dict:
    """sorted_candidates: [{candidate_id, composite_score}, ...] sorted descending."""
    if len(sorted_candidates) < 2:
        return {"gate_passed": len(sorted_candidates) == 1, "confidence_margin": 1.0}
    top, second = sorted_candidates[0], sorted_candidates[1]
    margin = top["composite_score"] - second["composite_score"]
    return {"gate_passed": margin >= min_margin, "confidence_margin": round(margin, 3)}