package com.example.dlqagent.domain;

public enum QuarantineStatus {
    /** Poison pill or rejected patch; a human should look. */
    PENDING_REVIEW,
    /** Tier 2: a valid repair is proposed and waits for approval. */
    PENDING_HUMAN_APPROVAL,
    /** Tier 2 approved by a human and replayed. */
    APPROVED_REPLAYED,
    /** Tier 2 proposal declined by a human. */
    REJECTED,
    RESOLVED,
    DISCARDED
}
