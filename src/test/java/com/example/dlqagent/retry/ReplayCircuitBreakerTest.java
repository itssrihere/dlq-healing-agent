package com.example.dlqagent.retry;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.dlqagent.infrastructure.DlqAgentProperties;
import com.example.dlqagent.retry.ReplayCircuitBreaker.State;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ReplayCircuitBreakerTest {

    private final MutableClock clock = new MutableClock();
    // 3 failures within 1 minute open it; 30s cooldown; 2 minute canary window
    private final ReplayCircuitBreaker breaker = new ReplayCircuitBreaker(clock,
            new DlqAgentProperties.Breaker(3, Duration.ofMinutes(1), Duration.ofSeconds(30), Duration.ofMinutes(2)));

    private void fail(int times) {
        for (int i = 0; i < times; i++) {
            breaker.recordRetryFailure();
        }
    }

    @Test
    void staysClosedBelowTheThreshold() {
        fail(2);
        assertThat(breaker.state()).isEqualTo(State.CLOSED);
        assertThat(breaker.tryAcquire()).isTrue();
    }

    @Test
    void failuresOutsideTheWindowDoNotAddUp() {
        fail(2);
        clock.advance(Duration.ofSeconds(61));
        fail(2);
        assertThat(breaker.state()).isEqualTo(State.CLOSED);
    }

    @Test
    void opensAtTheThresholdAndBlocksReplaysDuringTheCooldown() {
        fail(3);
        assertThat(breaker.state()).isEqualTo(State.OPEN);
        assertThat(breaker.tryAcquire()).isFalse();
        clock.advance(Duration.ofSeconds(29));
        assertThat(breaker.tryAcquire()).isFalse();
    }

    @Test
    void afterTheCooldownExactlyOneCanaryIsAllowed() {
        fail(3);
        clock.advance(Duration.ofSeconds(30));
        assertThat(breaker.tryAcquire()).isTrue();
        assertThat(breaker.state()).isEqualTo(State.HALF_OPEN);
        assertThat(breaker.tryAcquire()).isFalse();
    }

    @Test
    void aCanaryThatComesBackReopensTheBreakerForAFullCooldown() {
        fail(3);
        clock.advance(Duration.ofSeconds(30));
        assertThat(breaker.tryAcquire()).isTrue();
        breaker.recordRetryFailure();
        assertThat(breaker.state()).isEqualTo(State.OPEN);
        clock.advance(Duration.ofSeconds(29));
        assertThat(breaker.tryAcquire()).isFalse();
        clock.advance(Duration.ofSeconds(1));
        assertThat(breaker.tryAcquire()).isTrue();
    }

    @Test
    void aCanaryThatStaysQuietClosesTheBreakerAfterTheCanaryWindow() {
        fail(3);
        clock.advance(Duration.ofSeconds(30));
        assertThat(breaker.tryAcquire()).isTrue();
        clock.advance(Duration.ofSeconds(119));
        assertThat(breaker.state()).isEqualTo(State.HALF_OPEN);
        clock.advance(Duration.ofSeconds(1));
        assertThat(breaker.state()).isEqualTo(State.CLOSED);
        assertThat(breaker.tryAcquire()).isTrue();
        assertThat(breaker.tryAcquire()).isTrue();
    }

    @Test
    void anAbandonedCanaryIsNotMistakenForASuccess() {
        fail(3);
        clock.advance(Duration.ofSeconds(30));
        assertThat(breaker.tryAcquire()).isTrue();
        breaker.abandon(); // the broker refused the send
        clock.advance(Duration.ofMinutes(10));
        assertThat(breaker.state()).isEqualTo(State.HALF_OPEN); // not silently closed
        assertThat(breaker.tryAcquire()).isTrue(); // the next replay is the canary
    }

    @Test
    void closedAgainStartsCountingFromZero() {
        fail(3);
        clock.advance(Duration.ofSeconds(30));
        breaker.tryAcquire();
        clock.advance(Duration.ofMinutes(2));
        fail(2);
        assertThat(breaker.state()).isEqualTo(State.CLOSED);
    }
}
