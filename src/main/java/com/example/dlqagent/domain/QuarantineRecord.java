package com.example.dlqagent.domain;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * An event held for a human, with the failure metadata that explains why.
 * Either a quarantine (no proposal) or a Tier 2 approval request (proposedPayload set).
 */
public record QuarantineRecord(
        UUID id,
        String stream,
        String correlationId,
        String originalTopic,
        String key,
        String payload,
        Map<String, String> headers,
        String stackTrace,
        RemediationStrategy strategy,
        String reason,
        QuarantineStatus status,
        Instant createdAt,
        String proposedPayload,
        String repairSummary,
        List<String> riskFlags,
        String reviewedBy,
        Instant reviewedAt,
        String reviewNotes) {

    public QuarantineRecord {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        riskFlags = riskFlags == null ? List.of() : List.copyOf(riskFlags);
    }

    public static QuarantineRecord pending(FailureContext ctx, RemediationStrategy strategy, String reason) {
        return new QuarantineRecord(
                ctx.eventId(), ctx.stream(), ctx.correlationId(), ctx.originalTopic(), ctx.key(), ctx.payload(), ctx.headers(),
                ctx.stackTrace(), strategy, reason == null || reason.isBlank() ? "no reason given" : reason,
                QuarantineStatus.PENDING_REVIEW, Instant.now(), null, null, List.of(), null, null, null);
    }

    /** Tier 2: a repair that passed the hard checks but needs a human to accept it. */
    public static QuarantineRecord awaitingApproval(FailureContext ctx, RepairResult.Repaired repaired, String reason) {
        return new QuarantineRecord(
                ctx.eventId(), ctx.stream(), ctx.correlationId(), ctx.originalTopic(), ctx.key(), ctx.payload(), ctx.headers(),
                ctx.stackTrace(), RemediationStrategy.SCHEMA_DRIFT, reason, QuarantineStatus.PENDING_HUMAN_APPROVAL,
                Instant.now(), repaired.payload(), repaired.summary(), repaired.riskFlags(), null, null, null);
    }
}
