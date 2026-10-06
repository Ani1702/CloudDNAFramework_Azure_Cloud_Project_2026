from typing import List, Dict, Any, Tuple, Optional
import logging

logger = logging.getLogger("clouddna.confidence_gate")


class ConfidenceGate:
    def __init__(self, min_confidence_margin: float = 0.15):
        self.min_confidence_margin = min_confidence_margin

    def evaluate(
        self,
        ranked_candidates: List[Dict[str, Any]],
        min_margin_override: Optional[float] = None,
    ) -> Tuple[bool, Dict[str, Any], Optional[Dict[str, Any]], float]:
        """
        Evaluates the top-ranked candidates.
        Returns: (gate_passed, winning_candidate, runner_up_candidate, confidence_margin)
        """
        if not ranked_candidates:
            return False, {}, None, 0.0

        winner = ranked_candidates[0]
        margin_threshold = min_margin_override if min_margin_override is not None else self.min_confidence_margin

        # Case 1: Fast-path direct reuse or single validated candidate
        if len(ranked_candidates) == 1:
            score = winner["composite_score"]
            passed = score >= 0.70
            margin = round(score, 3)
            logger.info("Single candidate evaluation: Score=%.3f, Passed=%s", score, passed)
            return passed, winner, None, margin

        # Case 2: Multi-candidate competition (Member 2 sandbox)
        runner_up = ranked_candidates[1]
        score_1 = winner["composite_score"]
        score_2 = runner_up["composite_score"]
        margin = round(score_1 - score_2, 3)

        passed = margin >= margin_threshold
        logger.info(
            "Confidence Gate Evaluation: Rank 1 (%s)=%.3f, Rank 2 (%s)=%.3f, Margin=%.3f (Req: %.2f) -> Passed=%s",
            winner["candidate_id"], score_1, runner_up["candidate_id"], score_2, margin, margin_threshold, passed
        )

        return passed, winner, runner_up, margin