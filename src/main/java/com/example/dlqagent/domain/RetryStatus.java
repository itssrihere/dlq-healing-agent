package com.example.dlqagent.domain;

public enum RetryStatus {
    /** Waiting for {@code nextAttemptAt}. */
    SCHEDULED,
    /** Replayed; nothing has come back to the DLQ for it (yet). If it does, the row is rescheduled or exhausted. */
    REPLAYED,
    /** The attempt budget is spent; the event was quarantined. */
    EXHAUSTED
}
