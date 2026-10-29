package com.example.dlqagent.domain;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/** Port: durable Strategy B retries. Methods marked "locking" need an active transaction. */
public interface RetryScheduleRepository {

    /** @return true if a new row was written, false if one with the same fingerprint already exists */
    boolean insertIfAbsent(RetryEntry entry);

    /** Locking. */
    Optional<RetryEntry> lockByFingerprint(String fingerprint);

    /** Locking: the most overdue SCHEDULED row, skipping rows another transaction holds. */
    Optional<RetryEntry> lockNextDue(Instant now);

    void reschedule(UUID id, Instant nextAttemptAt, UUID lastEventId, String lastError);

    void markExhausted(UUID id, UUID lastEventId, String lastError);

    /** Records that replay number {@code attempt} was sent. */
    void markReplayed(UUID id, int attempt);
}
