package com.example.dlqagent.approval;

import com.example.dlqagent.domain.QuarantineRecord;
import com.example.dlqagent.domain.FieldChange;
import com.example.dlqagent.json.JsonDiff;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** What a reviewer sees: the original, the proposal, a field-level diff and why a human is needed. */
record ApprovalView(
        UUID id,
        String stream,
        String correlationId,
        String status,
        String originalTopic,
        String key,
        String reason,
        List<String> riskFlags,
        String repairSummary,
        String originalPayload,
        String proposedPayload,
        List<FieldChange> changes,
        Instant createdAt,
        String reviewedBy,
        Instant reviewedAt,
        String reviewNotes) {

    static ApprovalView of(QuarantineRecord r) {
        return new ApprovalView(r.id(), r.stream(), r.correlationId(), r.status().name(), r.originalTopic(), r.key(), r.reason(),
                r.riskFlags(), r.repairSummary(), r.payload(), r.proposedPayload(),
                r.proposedPayload() == null ? List.of() : JsonDiff.diffOrEmpty(r.payload(), r.proposedPayload()),
                r.createdAt(), r.reviewedBy(), r.reviewedAt(), r.reviewNotes());
    }
}
