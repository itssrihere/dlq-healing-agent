package com.example.dlqagent.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.domain.RetryDecision;
import com.example.dlqagent.domain.RetryEntry;
import com.example.dlqagent.domain.RetryScheduleRepository;
import com.example.dlqagent.domain.RetryStatus;
import com.example.dlqagent.stream.HealingStream;
import com.example.dlqagent.support.TestProps;
import com.example.dlqagent.support.TestStreams;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RetrySchedulerTest {

    private final RetryScheduleRepository repo = mock(RetryScheduleRepository.class);
    private final ReplayCircuitBreaker breaker = mock(ReplayCircuitBreaker.class);
    private final MutableClock clock = new MutableClock();
    private final HealingStream stream = TestStreams.orders(breaker);
    private final RetryScheduler scheduler = new RetryScheduler(repo, TestProps.defaults(), clock); // 5 replays, 2s x2
    private final FailureContext ctx = new FailureContext("orders", 
            "cid", "orders-dlq", 0, 1L, "orders-topic", "k", "{\"a\":1}", Map.of(), "trace", clock.instant());
    private final String fingerprint = RetryFingerprint.of("orders-topic", "k", "{\"a\":1}");

    private RetryEntry row(RetryStatus status, int attempt, FailureContext lastSeen) {
        return new RetryEntry(RetryEntry.idFor(fingerprint), "orders", fingerprint, "cid", "orders-topic", "k", "{\"a\":1}",
                Map.of(), attempt, clock.instant(), status, lastSeen.eventId(), "old reason");
    }

    private FailureContext later() {
        return new FailureContext("orders", "cid", "orders-dlq", 0, 2L, "orders-topic", "k", "{\"a\":1}", Map.of(), "t", clock.instant());
    }

    @Test
    void firstFailureSchedulesTheFirstReplayAfterTheInitialDelay() {
        when(repo.lockByFingerprint(fingerprint)).thenReturn(Optional.empty());
        when(repo.insertIfAbsent(any())).thenReturn(true);

        RetryDecision d = scheduler.recordFailure(stream, ctx, "conn reset");

        assertThat(d).isEqualTo(new RetryDecision.Scheduled(1, 5, clock.instant().plus(Duration.ofSeconds(2)), false));
        ArgumentCaptor<RetryEntry> saved = ArgumentCaptor.forClass(RetryEntry.class);
        verify(repo).insertIfAbsent(saved.capture());
        assertThat(saved.getValue().attempt()).isZero();
        assertThat(saved.getValue().payload()).isEqualTo("{\"a\":1}"); // unchanged
        verifyNoInteractions(breaker); // the original failure is not a failed retry
    }

    @Test
    void aFailedReplayIsRescheduledWithALongerDelayAndTellsTheBreaker() {
        FailureContext next = later();
        when(repo.lockByFingerprint(fingerprint)).thenReturn(Optional.of(row(RetryStatus.REPLAYED, 2, ctx)));

        RetryDecision d = scheduler.recordFailure(stream, next, "conn reset");

        Instant at = clock.instant().plus(Duration.ofSeconds(8)); // delayBefore(2) = 2s * 2^2
        assertThat(d).isEqualTo(new RetryDecision.Scheduled(3, 5, at, false));
        verify(repo).reschedule(RetryEntry.idFor(fingerprint), at, next.eventId(), "conn reset");
        verify(breaker).recordRetryFailure();
    }

    @Test
    void theAttemptBudgetIsSpentAfterMaxReplays() {
        FailureContext next = later();
        when(repo.lockByFingerprint(fingerprint)).thenReturn(Optional.of(row(RetryStatus.REPLAYED, 5, ctx)));

        RetryDecision d = scheduler.recordFailure(stream, next, "conn reset");

        assertThat(d).isInstanceOf(RetryDecision.Exhausted.class);
        assertThat(((RetryDecision.Exhausted) d).reason()).contains("5 replays").contains("conn reset");
        verify(repo).markExhausted(RetryEntry.idFor(fingerprint), next.eventId(), "conn reset");
        verify(repo, never()).reschedule(any(), any(), any(), any());
    }

    @Test
    void redeliveryOfTheSameDlqRecordChangesNothingAndIsNotAFailedRetry() {
        // The row was scheduled by this very record and may already have been replayed since.
        when(repo.lockByFingerprint(fingerprint)).thenReturn(Optional.of(row(RetryStatus.REPLAYED, 1, ctx)));

        assertThat(scheduler.recordFailure(stream, ctx, "conn reset")).isInstanceOf(RetryDecision.AlreadyHandled.class);

        verify(repo, never()).reschedule(any(), any(), any(), any());
        verify(repo, never()).markExhausted(any(), any(), any());
        verifyNoInteractions(breaker);
    }

    @Test
    void anIdenticalEventWhileAReplayIsPendingDoesNotSpendAnotherAttempt() {
        when(repo.lockByFingerprint(fingerprint)).thenReturn(Optional.of(row(RetryStatus.SCHEDULED, 0, ctx)));

        RetryDecision d = scheduler.recordFailure(stream, later(), "conn reset");

        assertThat(d).isInstanceOfSatisfying(RetryDecision.Scheduled.class, s -> {
            assertThat(s.duplicate()).isTrue();
            assertThat(s.replayNumber()).isEqualTo(1);
        });
        verify(repo, never()).reschedule(any(), any(), any(), any());
    }

    @Test
    void anExhaustedEventStaysExhausted() {
        when(repo.lockByFingerprint(fingerprint)).thenReturn(Optional.of(row(RetryStatus.EXHAUSTED, 5, ctx)));

        assertThat(scheduler.recordFailure(stream, later(), "conn reset")).isInstanceOf(RetryDecision.Exhausted.class);
        verify(repo, never()).reschedule(any(), any(), any(), any());
    }

    @Test
    void losingAnInsertRaceFallsBackToTheRowThatWon() {
        when(repo.lockByFingerprint(fingerprint)).thenReturn(Optional.empty(), Optional.of(row(RetryStatus.SCHEDULED, 0, later())));
        when(repo.insertIfAbsent(any())).thenReturn(false);

        RetryDecision d = scheduler.recordFailure(stream, ctx, "conn reset");

        assertThat(d).isInstanceOfSatisfying(RetryDecision.Scheduled.class, s -> assertThat(s.duplicate()).isTrue());
        verify(repo, never()).reschedule(any(), eq(clock.instant()), any(), any());
    }
}
