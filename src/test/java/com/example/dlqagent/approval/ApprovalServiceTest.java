package com.example.dlqagent.approval;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.dlqagent.agent.TopicAdapterRegistry;
import com.example.dlqagent.domain.AuditEntry;
import com.example.dlqagent.domain.AuditLog;
import com.example.dlqagent.domain.Evaluation;
import com.example.dlqagent.domain.EventReplayer;
import com.example.dlqagent.domain.QuarantineRecord;
import com.example.dlqagent.domain.QuarantineRepository;
import com.example.dlqagent.domain.QuarantineStatus;
import com.example.dlqagent.domain.RemediationStrategy;
import com.example.dlqagent.domain.ReplayRequest;
import com.example.dlqagent.domain.TopicAdapter;
import com.example.dlqagent.observability.TraceContext;
import io.micrometer.observation.ObservationRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ApprovalServiceTest {

    private static final UUID ID = UUID.randomUUID();

    private final QuarantineRepository repo = mock(QuarantineRepository.class);
    private final EventReplayer replayer = mock(EventReplayer.class);
    private final TopicAdapter adapter = mock(TopicAdapter.class);
    private final AuditLog audit = mock(AuditLog.class);
    private final ApprovalService service;

    ApprovalServiceTest() {
        when(adapter.topic()).thenReturn("orders-topic"); // must be stubbed before the registry reads it
        service = new ApprovalService(repo, new TopicAdapterRegistry(List.of(adapter)), replayer, audit,
                new TraceContext(null), ObservationRegistry.NOOP);
    }

    private static QuarantineRecord record(QuarantineStatus status) {
        return new QuarantineRecord(ID, "cid-1", "orders-topic", "k", "{\"orig\":1}", Map.of("tenant", "acme"), null,
                RemediationStrategy.SCHEMA_DRIFT, "Awaiting human approval: x", status, Instant.now(),
                "{\"proposed\":1}", "summary text", List.of("flag"), null, null, null);
    }

    private void locked(QuarantineStatus status) {
        when(repo.findForUpdate(ID)).thenReturn(Optional.of(record(status)));
        when(repo.findById(ID)).thenReturn(Optional.of(record(QuarantineStatus.APPROVED_REPLAYED)));
    }

    private void evaluates(Evaluation e) {
        when(adapter.evaluate("{\"orig\":1}", "{\"proposed\":1}", "k")).thenReturn(e);
    }

    @Test
    void approveReplaysTheProposalAttributedToTheReviewerThenMarksItDone() {
        locked(QuarantineStatus.PENDING_HUMAN_APPROVAL);
        evaluates(new Evaluation(List.of(), List.of("flag"))); // risk flags are what the human is accepting

        service.approve(ID, "alice", "looked fine");

        ArgumentCaptor<ReplayRequest> sent = ArgumentCaptor.forClass(ReplayRequest.class);
        verify(replayer).replay(sent.capture());
        assertThat(sent.getValue()).isEqualTo(new ReplayRequest("orders-topic", "k", "{\"proposed\":1}",
                Map.of("tenant", "acme"), "cid-1", "summary text", "alice"));
        verify(repo).markReviewed(ID, QuarantineStatus.APPROVED_REPLAYED, "alice", "looked fine");

        ArgumentCaptor<AuditEntry> entry = ArgumentCaptor.forClass(AuditEntry.class);
        verify(audit).record(entry.capture());
        AuditEntry a = entry.getValue();
        assertThat(a.stage()).isEqualTo(AuditEntry.Stage.APPROVAL);
        assertThat(a.outcome()).isEqualTo(AuditEntry.Outcome.APPROVED_REPLAYED);
        assertThat(a.tier()).isEqualTo(AuditEntry.Tier.TIER_2);
        assertThat(a.actor()).isEqualTo("alice");
        assertThat(a.eventId()).isEqualTo(ID);
        assertThat(a.id()).isEqualTo(AuditEntry.idFor(ID, AuditEntry.Stage.APPROVAL));
        assertThat(a.outcomeReason()).isEqualTo("Approved by alice: looked fine");
        assertThat(a.riskFlags()).containsExactly("flag");
        assertThat(a.repairSummary()).isEqualTo("summary text");
    }

    @Test
    void approveReChecksTheHardRulesAndRefusesIfTheyNoLongerHold() {
        locked(QuarantineStatus.PENDING_HUMAN_APPROVAL);
        evaluates(new Evaluation(List.of("amount value changed"), List.of()));

        assertThatThrownBy(() -> service.approve(ID, "alice", null))
                .isInstanceOf(ApprovalConflictException.class).hasMessageContaining("amount value changed");
        verifyNoInteractions(replayer, audit);
        verify(repo, never()).markReviewed(any(), any(), any(), any());
    }

    @Test
    void approveLeavesTheEventPendingIfTheBrokerDoesNotAcknowledge() {
        locked(QuarantineStatus.PENDING_HUMAN_APPROVAL);
        evaluates(new Evaluation(List.of(), List.of()));
        doThrow(new IllegalStateException("broker down")).when(replayer).replay(any());

        assertThatThrownBy(() -> service.approve(ID, "alice", null)).isInstanceOf(ReplayFailedException.class);
        verify(repo, never()).markReviewed(any(), any(), any(), any());
        verifyNoInteractions(audit); // a decision that did not happen is not audited as one
    }

    @Test
    void onlyPendingApprovalsCanBeApprovedOrRejected() {
        for (QuarantineStatus s : QuarantineStatus.values()) {
            if (s == QuarantineStatus.PENDING_HUMAN_APPROVAL) continue;
            locked(s);
            assertThatThrownBy(() -> service.approve(ID, "a", null)).as(s.name()).isInstanceOf(ApprovalConflictException.class);
            assertThatThrownBy(() -> service.reject(ID, "a", "no")).as(s.name()).isInstanceOf(ApprovalConflictException.class);
        }
        verifyNoInteractions(replayer);
        verify(repo, never()).markReviewed(any(), any(), any(), any());
    }

    @Test
    void plainQuarantinePendingReviewCannotBeApprovedBecauseItHasNoProposal() {
        locked(QuarantineStatus.PENDING_REVIEW);

        assertThatThrownBy(() -> service.approve(ID, "alice", null)).isInstanceOf(ApprovalConflictException.class);
    }

    @Test
    void unknownIdIsNotFound() {
        when(repo.findForUpdate(any())).thenReturn(Optional.empty());
        when(repo.findById(any())).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.approve(UUID.randomUUID(), "a", null)).isInstanceOf(ApprovalNotFoundException.class);
        assertThatThrownBy(() -> service.reject(UUID.randomUUID(), "a", "n")).isInstanceOf(ApprovalNotFoundException.class);
        assertThatThrownBy(() -> service.get(UUID.randomUUID())).isInstanceOf(ApprovalNotFoundException.class);
    }

    @Test
    void approveWithoutAnAdapterForTheTopicIsAConflict() {
        var other = new QuarantineRecord(ID, "cid", "payments-topic", "k", "{}", Map.of(), null,
                RemediationStrategy.SCHEMA_DRIFT, "r", QuarantineStatus.PENDING_HUMAN_APPROVAL, Instant.now(),
                "{}", "s", List.of(), null, null, null);
        when(repo.findForUpdate(ID)).thenReturn(Optional.of(other));

        assertThatThrownBy(() -> service.approve(ID, "a", null))
                .isInstanceOf(ApprovalConflictException.class).hasMessageContaining("payments-topic");
        verifyNoInteractions(replayer);
    }

    @Test
    void rejectRecordsTheDecisionAndNeverReplays() {
        locked(QuarantineStatus.PENDING_HUMAN_APPROVAL);

        service.reject(ID, "bob", "wrong date order");

        verify(repo).markReviewed(ID, QuarantineStatus.REJECTED, "bob", "wrong date order");
        verifyNoInteractions(replayer);
        ArgumentCaptor<AuditEntry> entry = ArgumentCaptor.forClass(AuditEntry.class);
        verify(audit).record(entry.capture());
        assertThat(entry.getValue().outcome()).isEqualTo(AuditEntry.Outcome.REJECTED);
        assertThat(entry.getValue().actor()).isEqualTo("bob");
        assertThat(entry.getValue().outcomeReason()).isEqualTo("Rejected by bob: wrong date order");
    }

    @Test
    void listClampsTheLimit() {
        service.list(QuarantineStatus.PENDING_HUMAN_APPROVAL, 100_000);
        service.list(QuarantineStatus.PENDING_HUMAN_APPROVAL, -5);

        verify(repo).findByStatus(QuarantineStatus.PENDING_HUMAN_APPROVAL, 100);
        verify(repo).findByStatus(eq(QuarantineStatus.PENDING_HUMAN_APPROVAL), eq(1));
        verify(repo, never()).findByStatus(any(), eq(100_000));
        verify(repo, never()).findByStatus(any(), eq(-5));
    }
}
