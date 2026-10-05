def adjust_weights(current_weights: dict, predicted_scores: dict, actual_post_promotion_scores: dict,
                    learning_rate: float = 0.05) -> dict:
    """If a domain's actual outcome matched its predicted score well, nudge its weight
    up slightly (the framework trusted that domain and was right). If it was off,
    nudge down. Simple, explainable, and easy to defend in a viva versus a black-box
    RL update — which matters given your own lit review flagged black-box tuning as
    a weakness in this space."""
    new_weights = {}
    for domain, w in current_weights.items():
        error = abs(predicted_scores.get(domain, 0) - actual_post_promotion_scores.get(domain, 0))
        accuracy = 1 - min(error, 1.0)
        new_weights[domain] = w + learning_rate * (accuracy - 0.5) * w
    total = sum(new_weights.values()) or 1.0
    return {d: round(w / total, 3) for d, w in new_weights.items()}   # renormalize to sum 1