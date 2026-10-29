package com.example.dlqagent.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.dlqagent.domain.AuditEntry;
import com.example.dlqagent.domain.AuditLog;
import com.example.dlqagent.domain.EventReplayer;
import com.example.dlqagent.domain.ReplayRequest;
import com.example.dlqagent.domain.RetryEntry;
import com.example.dlqagent.domain.RetryScheduleRepository;
import com.example.dlqagent.domain.RetryStatus;
import com.example.dlqagent.observability.TraceContext;
import com.example.dlqagent.support.TestProps;
import io.micrometer.observation.ObservationRegistry;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

class RetryPollerTest {

    private final RetryScheduleRepository repo = mock(RetryScheduleRepository.class);
    private final EventReplayer replayer = mock(EventReplayer.class);
    private final ReplayCircuitBreaker breaker = mock(ReplayCircuitBreaker.class);
    private final AuditLog audit = mock(AuditLog.class);
    private final PlatformTransactionManager txManager = mock(PlatformTransactionManager.class);
    private final TransactionStatus txStatus = mock(TransactionStatus.class);
    private final MutableClock clock = new MutableClock();
    private final RetryPoller poller;

    RetryPollerTest() {
        when(txManager.getTransaction(any())).thenReturn(txStatus);
        poller = new RetryPoller(repo, replayer, breaker, audit, new TransactionTemplate(txManager),
                ObservationRegistry.NOOP, new TraceContext(null), clock, TestProps.defaults()); // batch size 10
    }

    private RetryEntry due(int attempt) {
        return new RetryEntry(UUID.randomUUID(), "f", "cid", "orders-topic", "k", "{\"a\":1}",
                Map.of("tenant", "acme"), attempt, clock.instant(), RetryStatus.SCHEDULED, UUID.randomUUID(), "conn reset");
    }

    @Test
    void replaysTheOriginalPayloadUnchangedAndRecordsIt() {
        RetryEntry row = due(1);
        when(repo.lockNextDue(any())).thenReturn(Optional.of(row), Optional.empty());
        when(breaker.tryAcquire()).thenReturn(true);

        assertThat(poller.pollOnce()).isEqualTo(1);

        ArgumentCaptor<ReplayRequest> sent = ArgumentCaptor.forClass(ReplayRequest.class);
        verify(replayer).replay(sent.capture());
        assertThat(sent.getValue().payload()).isEqualTo("{\"a\":1}");
        assertThat(sent.getValue().key()).isEqualTo("k");
        assertThat(sent.getValue().topic()).isEqualTo("orders-topic");
        assertThat(sent.getValue().isRetry()).isTrue();
        assertThat(sent.getValue().retryAttempt()).isEqualTo(2); // the second replay
        verify(repo).markReplayed(row.id(), 2);
        ArgumentCaptor<AuditEntry> a = ArgumentCaptor.forClass(AuditEntry.class);
        verify(audit).record(a.capture());
        assertThat(a.getValue().stage()).isEqualTo(AuditEntry.Stage.RETRY);
        assertThat(a.getValue().outcome()).isEqualTo(AuditEntry.Outcome.RETRY_REPLAYED);
        assertThat(a.getValue().id()).isEqualTo(AuditEntry.idForRetry(row.id(), 2));
        assertThat(a.getValue().eventId()).isEqualTo(row.lastEventId());
    }

    @Test
    void anOpenBreakerLeavesTheRowDueAndRollsTheClaimBack() {
        when(repo.lockNextDue(any())).thenReturn(Optional.of(due(0)));
        when(breaker.tryAcquire()).thenReturn(false);

        assertThat(poller.pollOnce()).isZero();

        verify(txStatus).setRollbackOnly();
        verify(replayer, never()).replay(any());
        verify(repo, never()).markReplayed(any(), org.mockito.ArgumentMatchers.anyInt());
    }

    @Test
    void theBreakerIsNotAskedWhenNothingIsDue() {
        when(repo.lockNextDue(any())).thenReturn(Optional.empty());

        assertThat(poller.pollOnce()).isZero();

        verify(breaker, never()).tryAcquire(); // otherwise an idle poll would spend the half-open canary
    }

    @Test
    void aBrokerFailureReleasesTheCanaryAndLeavesTheRowUnmarked() {
        when(repo.lockNextDue(any())).thenReturn(Optional.of(due(0)));
        when(breaker.tryAcquire()).thenReturn(true);
        doThrow(new IllegalStateException("broker down")).when(replayer).replay(any());

        assertThatThrownBy(poller::pollOnce).isInstanceOf(IllegalStateException.class);

        verify(breaker).abandon();
        verify(repo, never()).markReplayed(any(), org.mockito.ArgumentMatchers.anyInt());
        verify(audit, never()).record(any());
    }

    @Test
    void scheduledPollNeverThrows() {
        when(repo.lockNextDue(any())).thenThrow(new IllegalStateException("db down"));

        poller.poll(); // must not propagate, or the schedule would stop
    }

    @Test
    void aBacklogIsSentInBatchesNotAllAtOnce() {
        when(repo.lockNextDue(any())).thenAnswer(i -> Optional.of(due(0)));
        when(breaker.tryAcquire()).thenReturn(true);

        assertThat(poller.pollOnce()).isEqualTo(10); // retry.batch-size
    }
}
