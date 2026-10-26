package com.example.dlqagent.domain;

import java.util.List;

/** Outcome of a Strategy A repair attempt. */
public sealed interface RepairResult {

    /**
     * A payload that passed the adapter's hard checks. Whether it may be replayed automatically is a
     * separate policy decision (see {@code riskFlags} and {@code confidence}).
     *
     * @param riskFlags  assumptions a human should confirm; empty for a purely mechanical fix
     * @param confidence the repair model's own confidence, 0 to 1; only ever used to demote
     * @param toolCalls  the tools the model called while working (names and outcomes only)
     * @param changes    field-level diff from the original payload to {@code payload}
     */
    record Repaired(String payload, String summary, List<String> riskFlags, double confidence,
                    List<ToolCallRecord> toolCalls, List<FieldChange> changes) implements RepairResult {

        public Repaired {
            riskFlags = List.copyOf(riskFlags);
            toolCalls = toolCalls == null ? List.of() : List.copyOf(toolCalls);
            changes = changes == null ? List.of() : List.copyOf(changes);
        }

        public Repaired(String payload, String summary, List<String> riskFlags, double confidence) {
            this(payload, summary, riskFlags, confidence, List.of(), List.of());
        }
    }

    /**
     * The event could not be repaired safely; it should be quarantined with this reason. The other fields say
     * what the agent had tried (all nullable or empty if it never got that far), for the audit trail.
     */
    record Unrepairable(String reason, String summary, Double confidence,
                        List<ToolCallRecord> toolCalls, List<FieldChange> attempted) implements RepairResult {

        public Unrepairable {
            toolCalls = toolCalls == null ? List.of() : List.copyOf(toolCalls);
            attempted = attempted == null ? List.of() : List.copyOf(attempted);
        }

        public Unrepairable(String reason) {
            this(reason, null, null, List.of(), List.of());
        }
    }
}
