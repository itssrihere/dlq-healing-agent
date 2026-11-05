package com.example.dlqagent.eval;

import java.util.List;

/**
 * What the real pipeline did with one case.
 *
 * @param plannedStrategy the planner's classification (SCHEMA_DRIFT, TRANSIENT, POISON_PILL), or null on an error
 * @param outcome         REPLAYED, HELD_FOR_APPROVAL, QUARANTINED, RETRY_SCHEDULED, or NONE if triage did not finish
 * @param finalPayload    the payload that was replayed (REPLAYED) or proposed to a human (HELD_FOR_APPROVAL), else null
 * @param error           non-null when the model or API failed, so the case says nothing about the model's judgement
 */
public record Observed(
        String plannedStrategy,
        String outcome,
        String finalPayload,
        List<String> riskFlags,
        Double plannerConfidence,
        Double repairConfidence,
        int toolCalls,
        long latencyMs,
        String reason,
        String error) {

    public Observed {
        riskFlags = riskFlags == null ? List.of() : List.copyOf(riskFlags);
    }
}
