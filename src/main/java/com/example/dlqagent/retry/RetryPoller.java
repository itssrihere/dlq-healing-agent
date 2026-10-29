package com.example.dlqagent.retry;

import com.example.dlqagent.domain.AuditEntry;
import com.example.dlqagent.domain.AuditLog;
import com.example.dlqagent.domain.EventReplayer;
import com.example.dlqagent.domain.RemediationStrategy;
import com.example.dlqagent.domain.ReplayRequest;
import com.example.dlqagent.domain.RetryEntry;
import com.example.dlqagent.domain.RetryScheduleRepository;
import com.example.dlqagent.infrastructure.DlqAgentProperties;
import com.example.dlqagent.observability.LogSafe;
import com.example.dlqagent.observability.TraceContext;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Replays due retries, unchanged. Each replay claims one row, asks the circuit breaker, sends, and records the
 * replay and its audit row in one transaction. Two instances cannot take the same row (the claim skips locked rows),
 * and a crash before commit leaves the row due, so it is replayed again: at-least-once, like every replay here.
 *
 * <p>Replays per wake-up are capped by {@code retry.batch-size}, which bounds the rate at which a backlog is sent.
 */
@Component
public class RetryPoller {

    private static final Logger log = LoggerFactory.getLogger(RetryPoller.class);
    private static final String AGENT = "agent";

    private final RetryScheduleRepository retries;
    private final EventReplayer replayer;
    private final ReplayCircuitBreaker breaker;
    private final AuditLog audit;
    private final TransactionTemplate tx;
    private final ObservationRegistry observations;
    private final TraceContext trace;
    private final Clock clock;
    private final int batchSize;

    public RetryPoller(RetryScheduleRepository retries, EventReplayer replayer, ReplayCircuitBreaker breaker,
                       AuditLog audit, TransactionTemplate tx, ObservationRegistry observations, TraceContext trace,
                       Clock clock, DlqAgentProperties props) {
        this.retries = retries;
        this.replayer = replayer;
        this.breaker = breaker;
        this.audit = audit;
        this.tx = tx;
        this.observations = observations;
        this.trace = trace;
        this.clock = clock;
        this.batchSize = props.retry().batchSize();
    }

    /** Scheduled entry point. Never throws: a broker or database outage must not stop the schedule. */
    void poll() {
        try {
            int sent = pollOnce();
            if (sent > 0) {
                log.info("Retry poller replayed {} event(s)", sent);
            }
        } catch (RuntimeException e) {
            log.warn("Retry poller failed, will try again at the next interval: {}", LogSafe.clean(e));
        }
    }

    /** @return how many events were replayed */
    public int pollOnce() {
        int sent = 0;
        while (sent < batchSize && Boolean.TRUE.equals(tx.execute(this::replayNextDue))) {
            sent++;
        }
        return sent;
    }

    private boolean replayNextDue(TransactionStatus status) {
        long started = System.nanoTime();
        Optional<RetryEntry> due = retries.lockNextDue(clock.instant());
        if (due.isEmpty()) {
            return false;
        }
        if (!breaker.tryAcquire()) {
            // Open breaker: leave the row due; it goes out once the breaker lets replays through again.
            status.setRollbackOnly();
            return false;
        }
        RetryEntry row = due.get();
        int replayNumber = row.attempt() + 1;
        try {
            Observation.createNotStarted("dlq.retry", observations)
                    .lowCardinalityKeyValue("topic", row.targetTopic())
                    .highCardinalityKeyValue("correlation.id", row.correlationId())
                    .highCardinalityKeyValue("retry.attempt", Integer.toString(replayNumber))
                    .observe(() -> {
                        replayer.replay(ReplayRequest.retry(row, replayNumber));
                        retries.markReplayed(row.id(), replayNumber);
                        audit.record(auditEntry(row, replayNumber, started));
                    });
        } catch (RuntimeException e) {
            breaker.abandon();
            throw e;
        }
        return true;
    }

    private AuditEntry auditEntry(RetryEntry row, int replayNumber, long startedNanos) {
        return new AuditEntry(
                AuditEntry.idForRetry(row.id(), replayNumber), row.lastEventId(), row.correlationId(),
                trace.traceId(), Instant.now(), AuditEntry.Stage.RETRY, row.targetTopic(), row.key(),
                RemediationStrategy.TRANSIENT, null, row.lastError(), null, null, List.of(), List.of(), List.of(),
                AuditEntry.Tier.NONE, AuditEntry.Outcome.RETRY_REPLAYED,
                "Replayed unchanged (replay %d)".formatted(replayNumber), AGENT,
                (System.nanoTime() - startedNanos) / 1_000_000);
    }
}
