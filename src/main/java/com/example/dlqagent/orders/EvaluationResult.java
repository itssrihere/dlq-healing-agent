package com.example.dlqagent.orders;

import java.util.List;

/**
 * Verdict of the order contract validator. {@code event} is the typed, validated payload when there are no violations.
 *
 * @param violations hard failures; the candidate must not be replayed
 * @param riskFlags  assumptions a human should confirm before the candidate is replayed
 */
public record EvaluationResult(List<String> violations, List<String> riskFlags, OrderEvent event) {

    public EvaluationResult {
        violations = List.copyOf(violations);
        riskFlags = List.copyOf(riskFlags);
    }

    public boolean passed() {
        return violations.isEmpty();
    }
}
