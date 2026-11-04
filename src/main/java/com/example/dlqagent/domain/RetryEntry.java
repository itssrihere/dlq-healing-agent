package com.example.dlqagent.domain;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * One event waiting for, or having had, Strategy B retries. The payload is replayed unchanged.
 *
 * @param fingerprint  identity of the event's content (see {@code RetryFingerprint}); unique per row
 * @param attempt      replays already sent
 * @param lastEventId  the DLQ record that last scheduled this retry
 * @param lastError    the planner's reason for calling the failure transient; untrusted model output
 */
public record RetryEntry(
        UUID id,
        String stream,
        String fingerprint,
        String correlationId,
        String targetTopic,
        String key,
        String payload,
        Map<String, String> headers,
        int attempt,
        Instant nextAttemptAt,
        RetryStatus status,
        UUID lastEventId,
        String lastError) {

    public RetryEntry {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    /** Deterministic from the fingerprint, so two racing inserts for the same content collide instead of doubling. */
    public static UUID idFor(String fingerprint) {
        return UUID.nameUUIDFromBytes(("retry:" + fingerprint).getBytes(StandardCharsets.UTF_8));
    }

    public static RetryEntry first(FailureContext ctx, String fingerprint, Instant nextAttemptAt, String reason) {
        return new RetryEntry(idFor(fingerprint), ctx.stream(), fingerprint, ctx.correlationId(), ctx.originalTopic(), ctx.key(),
                ctx.payload(), ctx.headers(), 0, nextAttemptAt, RetryStatus.SCHEDULED, ctx.eventId(), reason);
    }
}
