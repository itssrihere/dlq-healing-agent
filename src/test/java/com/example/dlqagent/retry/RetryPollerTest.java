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
import com.example.dlqagent.stream.HealingStream;
import com.example.dlqagent.stream.StreamRegistry;
import com.example.dlqagent.support.TestProps;
import com.example.dlqagent.support.TestStreams;
import io.micrometer.observation.ObservationRegistry;
import java.util.List;
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
    private final StreamRegistry streams = mock(StreamRegistry.class);
    private final AuditLog audit = mock(AuditLog.class);
    private final PlatformTransactionManager txManager = mock(PlatformTransactionManager.class);
    private final TransactionStatus txStatus = mock(TransactionStatus.class);
    private final MutableClock clock = new MutableClock();
    private final RetryPoller poller;

    RetryPollerTest() {
        when(txManager.getTransaction(any())).thenReturn(txStatus);
        when(streams.all()).thenReturn(List.of(TestStreams.orders(breaker)));
        poller = new RetryPoller(repo, replayer, streams, audit, new TransactionTemplate(txManager),
                ObservationRegistry.NOOP, new TraceContext(null), clock, TestProps.defaults()); // batch size 10
    }

    private RetryEntry due(int attempt) {
        return new RetryEntry(UUID.randomUUID(), "orders", "f", "cid", "orders-topic", "k", "{\"a\":1}",
                Map.of("tenant", "acme"), attempt, clock.instant(), RetryStatus.SCHEDULED, UUID.randomUUID(), "conn reset");
    }

    @Test
    void replaysTheOriginalPayloadUnchangedAndRecordsIt() {
        RetryEntry row = due(1);
        when(repo.lockNextDue(any(), any(), any())).thenReturn(Optional.of(row), Optional.empty());
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
        when(repo.lockNextDue(any(), any(), any())).thenReturn(Optional.of(due(0)));
        when(breaker.tryAcquire()).thenReturn(false);

        assertThat(poller.pollOnce()).isZero();

        verify(txStatus).setRollbackOnly();
        verify(replayer, never()).replay(any());
        verify(repo, never()).markReplayed(any(), org.mockito.ArgumentMatchers.anyInt());
    }

    @Test
    void theBreakerIsNotAskedWhenNothingIsDue() {
        when(repo.lockNextDue(any(), any(), any())).thenReturn(Optional.empty());

        assertThat(poller.pollOnce()).isZero();

        verify(breaker, never()).tryAcquire(); // otherwise an idle poll would spend the half-open canary
    }

    @Test
    void aBrokerFailureReleasesTheCanaryAndLeavesTheRowUnmarked() {
        when(repo.lockNextDue(any(), any(), any())).thenReturn(Optional.of(due(0)));
        when(breaker.tryAcquire()).thenReturn(true);
        doThrow(new IllegalStateException("broker down")).when(replayer).replay(any());

        assertThatThrownBy(poller::pollOnce).isInstanceOf(IllegalStateException.class);

        verify(breaker).abandon();
        verify(repo, never()).markReplayed(any(), org.mockito.ArgumentMatchers.anyInt());
        verify(audit, never()).record(any());
    }

    @Test
    void scheduledPollNeverThrows() {
        when(repo.lockNextDue(any(), any(), any())).thenThrow(new IllegalStateException("db down"));

        poller.poll(); // must not propagate, or the schedule would stop
    }

    @Test
    void aBacklogIsSentInBatchesNotAllAtOnce() {
        when(repo.lockNextDue(any(), any(), any())).thenAnswer(i -> Optional.of(due(0)));
        when(breaker.tryAcquire()).thenReturn(true);

        assertThat(poller.pollOnce()).isEqualTo(10); // retry.batch-size
    }

    @Test
    void oneStreamsOpenBreakerDoesNotHoldBackAnotherStreamsRetries() {
        ReplayCircuitBreaker shipmentsBreaker = mock(ReplayCircuitBreaker.class);
        var shipments = new HealingStream("shipments", "shipments-dlq", "shipments-topic", "g",
                TestStreams.orders().adapter(), true, 0.9, 5, shipmentsBreaker);
        when(streams.all()).thenReturn(List.of(TestStreams.orders(breaker), shipments));
        RetryEntry ordersRow = due(0);
        RetryEntry shipmentsRow = new RetryEntry(UUID.randomUUID(), "shipments", "f2", "cid2", "shipments-topic", "s-1",
                "{\"a\":2}", Map.of(), 0, clock.instant(), RetryStatus.SCHEDULED, UUID.randomUUID(), "conn reset");
        when(repo.lockNextDue(any(), org.mockito.ArgumentMatchers.eq("orders"), any())).thenReturn(Optional.of(ordersRow));
        when(repo.lockNextDue(any(), org.mockito.ArgumentMatchers.eq("shipments"), any()))
                .thenReturn(Optional.of(shipmentsRow), Optional.empty());
        when(breaker.tryAcquire()).thenReturn(false); // orders: open
        when(shipmentsBreaker.tryAcquire()).thenReturn(true);

        assertThat(poller.pollOnce()).isEqualTo(1);

        ArgumentCaptor<ReplayRequest> sent = ArgumentCaptor.forClass(ReplayRequest.class);
        verify(replayer).replay(sent.capture());
        assertThat(sent.getValue().topic()).isEqualTo("shipments-topic");
        // asked once to find the row and once more to learn the stream's batch was done
        verify(repo, org.mockito.Mockito.atLeastOnce()).lockNextDue(any(), org.mockito.ArgumentMatchers.eq("shipments"),
                org.mockito.ArgumentMatchers.eq("shipments-topic"));
    }

    @Test
    void eachStreamIsAskedForRowsAimedAtItsOwnSourceTopicOnly() {
        when(repo.lockNextDue(any(), any(), any())).thenReturn(Optional.empty());

        poller.pollOnce();

        verify(repo).lockNextDue(any(), org.mockito.ArgumentMatchers.eq("orders"),
                org.mockito.ArgumentMatchers.eq("orders-topic"));
    }
}
