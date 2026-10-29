package com.example.dlqagent.retry;

import com.example.dlqagent.infrastructure.DlqAgentProperties;
import com.example.dlqagent.observability.LogSafe;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stops replays when retried events keep coming back to the DLQ, so a downstream that is still down is not
 * hammered. It watches outcomes only; it knows nothing about the downstream's internals.
 *
 * <ul>
 *   <li>CLOSED: replays flow. {@code failureThreshold} retried events returning within {@code failureWindow} open it.
 *   <li>OPEN: no replays for {@code cooldown}.
 *   <li>HALF_OPEN: one canary replay is allowed. If a retried event comes back, OPEN again. If nothing comes back
 *       within {@code canaryWindow}, CLOSED. "Nothing came back" is absence of evidence, not proof of success.
 * </ul>
 *
 * State is in memory on purpose: a restart resets it to CLOSED, which costs at most one extra failing replay. What
 * must survive a restart, the per-event attempt budget, is in the database.
 */
public final class ReplayCircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private static final Logger log = LoggerFactory.getLogger(ReplayCircuitBreaker.class);

    private final Clock clock;
    private final int failureThreshold;
    private final Duration failureWindow;
    private final Duration cooldown;
    private final Duration canaryWindow;

    private final Deque<Instant> failures = new ArrayDeque<>();
    private State state = State.CLOSED;
    private Instant openedAt = Instant.EPOCH;
    private boolean canaryOutstanding;
    private Instant canarySentAt = Instant.EPOCH;

    public ReplayCircuitBreaker(Clock clock, DlqAgentProperties.Breaker config) {
        this.clock = clock;
        this.failureThreshold = config.failureThreshold();
        this.failureWindow = config.failureWindow();
        this.cooldown = config.cooldown();
        this.canaryWindow = config.canaryWindow();
    }

    /** A retried event came back to the DLQ with a transient failure. */
    public synchronized void recordRetryFailure() {
        Instant now = clock.instant();
        refresh(now);
        switch (state) {
            case HALF_OPEN -> open(now, "the canary replay came back");
            case CLOSED -> {
                failures.addLast(now);
                prune(now);
                if (failures.size() >= failureThreshold) {
                    open(now, failures.size() + " retried events came back within " + failureWindow);
                }
            }
            case OPEN -> { /* already stopped */ }
        }
    }

    /** Ask before each replay. A true in HALF_OPEN is the canary; report {@link #abandon()} if it was not sent. */
    public synchronized boolean tryAcquire() {
        Instant now = clock.instant();
        refresh(now);
        if (state == State.OPEN && !now.isBefore(openedAt.plus(cooldown))) {
            state = State.HALF_OPEN;
            canaryOutstanding = false;
            log.info("Replay circuit breaker half-open: sending one canary replay");
        }
        return switch (state) {
            case CLOSED -> true;
            case OPEN -> false;
            case HALF_OPEN -> {
                if (canaryOutstanding) {
                    yield false;
                }
                canaryOutstanding = true;
                canarySentAt = now;
                yield true;
            }
        };
    }

    /** The replay that {@link #tryAcquire()} allowed was not sent (broker error); let the next one be the canary. */
    public synchronized void abandon() {
        if (state == State.HALF_OPEN) {
            canaryOutstanding = false;
        }
    }

    public synchronized State state() {
        refresh(clock.instant());
        return state;
    }

    /** Time passing can close a half-open breaker: the canary went out and nothing came back. */
    private void refresh(Instant now) {
        if (state == State.HALF_OPEN && canaryOutstanding && !now.isBefore(canarySentAt.plus(canaryWindow))) {
            state = State.CLOSED;
            canaryOutstanding = false;
            failures.clear();
            log.info("Replay circuit breaker closed: the canary replay did not come back within {}", LogSafe.clean(canaryWindow));
        }
    }

    private void open(Instant now, String why) {
        state = State.OPEN;
        openedAt = now;
        canaryOutstanding = false;
        failures.clear();
        log.warn("Replay circuit breaker open for {}: {}", LogSafe.clean(cooldown), LogSafe.clean(why));
    }

    private void prune(Instant now) {
        Instant cutoff = now.minus(failureWindow);
        while (!failures.isEmpty() && failures.peekFirst().isBefore(cutoff)) {
            failures.removeFirst();
        }
    }
}
