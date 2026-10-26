package com.example.dlqagent.domain;

import java.util.List;

/**
 * A topic adapter's verdict on a candidate repair.
 *
 * @param violations hard failures: the candidate must never be replayed, automatically or by approval
 * @param riskFlags  things a human should confirm: the candidate is valid but rests on an assumption
 */
public record Evaluation(List<String> violations, List<String> riskFlags) {

    public Evaluation {
        violations = List.copyOf(violations);
        riskFlags = List.copyOf(riskFlags);
    }

    public boolean rejected() {
        return !violations.isEmpty();
    }
}
