package com.example.dlqagent.domain;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * One row of the append-only audit log: what the agent (or a human) decided about one event, and why.
 * Holds reasoning summaries, tool-call summaries and field-level diffs, never raw prompts.
 *
 * @param plannerConfidence null when no planner decision was made (for example the replay-loop guard)
 * @param actor             {@code "agent"}, or the reviewer's username for an approval stage entry
 */
public record AuditEntry(
        UUID id,
        UUID eventId,
        String correlationId,
        String traceId,
        Instant occurredAt,
        Stage stage,
        String originalTopic,
        String key,
        RemediationStrategy strategy,
        Double plannerConfidence,
        String plannerRationale,
        String repairSummary,
        Double repairConfidence,
        List<ToolCallRecord> toolCalls,
        List<FieldChange> changes,
        List<String> riskFlags,
        Tier tier,
        Outcome outcome,
        String outcomeReason,
        String actor,
        long durationMs) {

    public enum Stage { TRIAGE, APPROVAL, RETRY }

    public enum Tier { NONE, TIER_1, TIER_2 }

    public enum Outcome {
        REPLAYED, HELD_FOR_APPROVAL, QUARANTINED, APPROVED_REPLAYED, REJECTED,
        /** Strategy B: a replay was scheduled for later. */
        RETRY_SCHEDULED,
        /** Strategy B: a scheduled replay was sent, unchanged. */
        RETRY_REPLAYED
    }

    public AuditEntry {
        toolCalls = toolCalls == null ? List.of() : List.copyOf(toolCalls);
        changes = changes == null ? List.of() : List.copyOf(changes);
        riskFlags = riskFlags == null ? List.of() : List.copyOf(riskFlags);
    }

    /** Deterministic per (event, stage), so reprocessing the same DLQ record cannot write a second row. */
    public static UUID idFor(UUID eventId, Stage stage) {
        return UUID.nameUUIDFromBytes((eventId + ":" + stage).getBytes(StandardCharsets.UTF_8));
    }

    /** One row per retry replay: deterministic per (retry, replay number), so a redone replay cannot write two. */
    public static UUID idForRetry(UUID retryId, int replayNumber) {
        return UUID.nameUUIDFromBytes((retryId + ":replay:" + replayNumber).getBytes(StandardCharsets.UTF_8));
    }
}
