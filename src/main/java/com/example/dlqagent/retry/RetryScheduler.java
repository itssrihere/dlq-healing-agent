package com.example.dlqagent.retry;

import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.domain.RetryDecision;
import com.example.dlqagent.domain.RetryEntry;
import com.example.dlqagent.domain.RetryScheduleRepository;
import com.example.dlqagent.infrastructure.DlqAgentProperties;
import com.example.dlqagent.stream.HealingStream;
import java.time.Clock;
import java.time.Instant;
import org.springframework.stereotype.Service;

/**
 * Decides what a transient failure leads to: a first retry, a later retry, or exhaustion. Deterministic; the model
 * only said "transient", it has no say in how often or how long to retry.
 *
 * <p>Every method must run inside the caller's transaction, so the retry row, the quarantine row and the audit
 * row commit together.
 */
@Service
public class RetryScheduler {

    private final RetryScheduleRepository retries;
    private final Backoff backoff;
    private final Clock clock;

    /** The backoff curve is shared; the attempt budget and the circuit breaker belong to the stream. */
    public RetryScheduler(RetryScheduleRepository retries, DlqAgentProperties props, Clock clock) {
        this.retries = retries;
        this.backoff = new Backoff(props.retry());
        this.clock = clock;
    }

    public RetryDecision recordFailure(HealingStream stream, FailureContext ctx, String reason) {
        int maxAttempts = stream.maxAttempts();
        String fingerprint = RetryFingerprint.of(ctx.originalTopic(), ctx.key(), ctx.payload());
        Instant now = clock.instant();

        RetryEntry row = retries.lockByFingerprint(fingerprint).orElse(null);
        if (row == null) {
            RetryEntry fresh = RetryEntry.first(ctx, fingerprint, now.plus(backoff.delayBefore(0)), reason);
            if (retries.insertIfAbsent(fresh)) {
                return new RetryDecision.Scheduled(1, maxAttempts, fresh.nextAttemptAt(), false);
            }
            // Lost a race with a concurrent insert of identical content; treat it as the row that exists now.
            row = retries.lockByFingerprint(fingerprint).orElseThrow();
        }

        if (ctx.eventId().equals(row.lastEventId())) {
            return new RetryDecision.AlreadyHandled();
        }
        return switch (row.status()) {
            // Another DLQ record with identical content while a replay is already pending: one replay covers both.
            case SCHEDULED -> new RetryDecision.Scheduled(row.attempt() + 1, maxAttempts, row.nextAttemptAt(), true);
            case EXHAUSTED -> new RetryDecision.Exhausted("Retries for this event were already exhausted");
            case REPLAYED -> afterFailedReplay(stream, row, ctx, reason, now);
        };
    }

    private RetryDecision afterFailedReplay(HealingStream stream, RetryEntry row, FailureContext ctx, String reason,
                                            Instant now) {
        int maxAttempts = stream.maxAttempts();
        stream.breaker().recordRetryFailure();
        if (row.attempt() >= maxAttempts) {
            retries.markExhausted(row.id(), ctx.eventId(), reason);
            return new RetryDecision.Exhausted(
                    "Still failing after %d replays: %s".formatted(row.attempt(), reason));
        }
        Instant next = now.plus(backoff.delayBefore(row.attempt()));
        retries.reschedule(row.id(), next, ctx.eventId(), reason);
        return new RetryDecision.Scheduled(row.attempt() + 1, maxAttempts, next, false);
    }
}
