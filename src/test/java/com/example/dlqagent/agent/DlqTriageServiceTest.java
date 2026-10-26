package com.example.dlqagent.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.dlqagent.domain.AuditEntry;
import com.example.dlqagent.domain.AuditLog;
import com.example.dlqagent.domain.EventReplayer;
import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.domain.FieldChange;
import com.example.dlqagent.domain.PayloadRepairer;
import com.example.dlqagent.domain.QuarantineRecord;
import com.example.dlqagent.domain.QuarantineRepository;
import com.example.dlqagent.domain.QuarantineStatus;
import com.example.dlqagent.domain.RemediationPlan;
import com.example.dlqagent.domain.RemediationPlanner;
import com.example.dlqagent.domain.RemediationStrategy;
import com.example.dlqagent.domain.ReplayRequest;
import com.example.dlqagent.domain.RepairResult;
import com.example.dlqagent.domain.ToolCallRecord;
import com.example.dlqagent.observability.TraceContext;
import com.example.dlqagent.support.TestProps;
import io.micrometer.observation.ObservationRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class DlqTriageServiceTest {

    private final RemediationPlanner planner = mock(RemediationPlanner.class);
    private final PayloadRepairer repairer = mock(PayloadRepairer.class);
    private final EventReplayer replayer = mock(EventReplayer.class);
    private final QuarantineRepository repo = mock(QuarantineRepository.class);
    private final AuditLog audit = mock(AuditLog.class);
    private final PlatformTransactionManager txManager = mock(PlatformTransactionManager.class);
    private final DlqTriageService service = new DlqTriageService(planner, repairer, replayer, repo, audit,
            TestProps.defaults(), ObservationRegistry.NOOP, new TransactionTemplate(txManager), new TraceContext(null));
    // auto-replay needs >= 0.9
    private final FailureContext ctx = new FailureContext(
            "cid-1", "orders-dlq", 0, 7L, "orders-topic", "k", "{bad", Map.of("h", "v"), "trace", Instant.now());

    private QuarantineRecord saved() {
        ArgumentCaptor<QuarantineRecord> captor = ArgumentCaptor.forClass(QuarantineRecord.class);
        verify(repo).save(captor.capture());
        return captor.getValue();
    }

    private static final List<ToolCallRecord> TOOLS = List.of(new ToolCallRecord("describeTable", true, 3));
    private static final List<FieldChange> CHANGES = List.of(new FieldChange("amount", "\"19.99 USD\"", "19.99"));

    private static RepairResult.Repaired repaired(double confidence, String... flags) {
        return new RepairResult.Repaired("{\"fixed\":true}", "amount to number", List.of(flags), confidence, TOOLS, CHANGES);
    }

    private AuditEntry audited() {
        ArgumentCaptor<AuditEntry> captor = ArgumentCaptor.forClass(AuditEntry.class);
        verify(audit).record(captor.capture());
        return captor.getValue();
    }

    private void planRepair(double planConfidence, RepairResult result) {
        when(planner.plan(ctx)).thenReturn(new RemediationPlan.SchemaRepair("amount as text", planConfidence));
        when(repairer.repair(ctx)).thenReturn(result);
    }

    // ---- Strategy C ---------------------------------------------------------------------------------------

    @Test
    void quarantinePlanIsPersistedAsPoisonPill() {
        when(planner.plan(ctx)).thenReturn(new RemediationPlan.Quarantine("corrupt"));

        service.triage(ctx);

        QuarantineRecord r = saved();
        assertThat(r.strategy()).isEqualTo(RemediationStrategy.POISON_PILL);
        assertThat(r.status()).isEqualTo(QuarantineStatus.PENDING_REVIEW);
        assertThat(r.reason()).isEqualTo("corrupt");
        assertThat(r.id()).isEqualTo(ctx.eventId());
        assertThat(r.proposedPayload()).isNull();
    }

    // ---- Tier 1 -------------------------------------------------------------------------------------------

    @Test
    void riskFreeHighConfidenceRepairIsReplayedAutomatically() {
        var repaired = repaired(0.95);
        planRepair(0.95, repaired);

        service.triage(ctx);

        verify(replayer).replay(ReplayRequest.automatic(ctx, repaired));
        verifyNoInteractions(repo);
    }

    @Test
    void confidenceExactlyAtTheThresholdIsStillTier1() {
        planRepair(0.9, repaired(0.9));

        service.triage(ctx);

        verify(replayer).replay(any());
        verifyNoInteractions(repo);
    }

    // ---- Tier 2 -------------------------------------------------------------------------------------------

    @Test
    void anyRiskFlagSendsTheRepairToHumanApprovalEvenAtFullConfidence() {
        var repaired = repaired(1.0, "createdAt 'x' is ambiguous");
        planRepair(1.0, repaired);

        service.triage(ctx);

        QuarantineRecord r = saved();
        assertThat(r.status()).isEqualTo(QuarantineStatus.PENDING_HUMAN_APPROVAL);
        assertThat(r.strategy()).isEqualTo(RemediationStrategy.SCHEMA_DRIFT);
        assertThat(r.proposedPayload()).isEqualTo("{\"fixed\":true}");
        assertThat(r.payload()).isEqualTo("{bad"); // the original is kept for the reviewer's diff
        assertThat(r.riskFlags()).containsExactly("createdAt 'x' is ambiguous");
        assertThat(r.repairSummary()).isEqualTo("amount to number");
        assertThat(r.reason()).startsWith("Awaiting human approval").contains("ambiguous");
        verifyNoInteractions(replayer);
    }

    @Test
    void lowRepairConfidenceDemotesAnOtherwiseCleanRepair() {
        planRepair(0.99, repaired(0.8));

        service.triage(ctx);

        assertThat(saved().reason()).contains("repair confidence 0.80 is below the auto-replay threshold 0.90");
        verifyNoInteractions(replayer);
    }

    @Test
    void plannerConfidenceDoesNotDecideTheTierOnceItHasClearedTheRoutingBar() {
        // Real finding: the real planner says ~0.80 for clean drift. That is about classification, not about this patch.
        var repaired = repaired(0.95);
        planRepair(0.75, repaired);

        service.triage(ctx);

        verify(replayer).replay(ReplayRequest.automatic(ctx, repaired));
        verifyNoInteractions(repo);
    }

    @Test
    void aConfidentPlannerCannotRescueALowConfidenceRepair() {
        planRepair(1.0, repaired(0.85));

        service.triage(ctx);

        assertThat(saved().reason()).contains("repair confidence 0.85");
        verifyNoInteractions(replayer);
    }

    @Test
    void reasonListsEveryCauseForHumanApproval() {
        planRepair(0.95, repaired(0.5, "flag one", "flag two"));

        service.triage(ctx);

        assertThat(saved().reason()).contains("flag one", "flag two", "repair confidence 0.50 is below the auto-replay threshold");
    }

    // ---- fail-closed paths ---------------------------------------------------------------------------------

    @Test
    void unrepairableEventIsQuarantinedAsSchemaDriftAndNotReplayed() {
        planRepair(0.9, new RepairResult.Unrepairable("gate said no"));

        service.triage(ctx);

        QuarantineRecord r = saved();
        assertThat(r.strategy()).isEqualTo(RemediationStrategy.SCHEMA_DRIFT);
        assertThat(r.status()).isEqualTo(QuarantineStatus.PENDING_REVIEW);
        assertThat(r.reason()).isEqualTo("gate said no");
        verifyNoInteractions(replayer);
    }

    @Test
    void repairerExceptionAndNullFailClosed() {
        when(planner.plan(ctx)).thenReturn(new RemediationPlan.SchemaRepair("x", 0.9));
        when(repairer.repair(ctx)).thenThrow(new IllegalStateException("boom")).thenReturn(null);

        service.triage(ctx);
        service.triage(ctx);

        verify(repo, times(2)).save(any());
        verifyNoInteractions(replayer);
    }

    @Test
    void replayFailureIsNotSwallowedSoTheListenerRetries() {
        planRepair(0.95, repaired(0.95));
        doThrow(new IllegalStateException("broker down")).when(replayer).replay(any());

        assertThatThrownBy(() -> service.triage(ctx)).isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(repo);
    }

    @Test
    void neverReplaysOrQueuesForATopicOtherThanTheMainTopic() {
        var spoofed = new FailureContext("cid-1", "orders-dlq", 0, 7L, "payments-topic", "k", "{}", Map.of(), null,
                Instant.now());
        when(planner.plan(spoofed)).thenReturn(new RemediationPlan.SchemaRepair("x", 0.9));

        service.triage(spoofed);

        assertThat(saved().reason()).contains("unexpected topic");
        verifyNoInteractions(repairer, replayer);
    }

    @Test
    void alreadyRepairedEventGoesStraightToQuarantineWithoutPlanning() {
        var again = new FailureContext("cid-1", "orders-dlq", 0, 7L, "orders-topic", "k", "{}",
                Map.of("x-repaired-by-agent", "true"), null, Instant.now());

        service.triage(again);

        assertThat(saved().reason()).contains("already repaired");
        verifyNoInteractions(planner, repairer, replayer);
    }

    @Test
    void transientStrategyStillFailsClosedUntilItIsImplemented() {
        when(planner.plan(ctx)).thenReturn(new RemediationPlan.TransientRetry("conn reset", 0.9));

        service.triage(ctx);

        QuarantineRecord r = saved();
        assertThat(r.strategy()).isEqualTo(RemediationStrategy.TRANSIENT);
        assertThat(r.reason()).contains("not implemented");
        verify(replayer, never()).replay(any());
    }

    @Test
    void plannerExceptionAndNullPlanFailClosed() {
        when(planner.plan(any())).thenThrow(new IllegalStateException("llm down")).thenReturn(null);

        service.triage(ctx);
        service.triage(ctx);

        ArgumentCaptor<QuarantineRecord> captor = ArgumentCaptor.forClass(QuarantineRecord.class);
        verify(repo, times(2)).save(captor.capture());
        assertThat(captor.getAllValues().get(0).reason()).contains("Planner failed");
        assertThat(captor.getAllValues().get(1).reason()).contains("no plan");
    }

    // ---- audit trail ----------------------------------------------------------------------------------------

    @Test
    void tier1WritesAnAuditRowWithTheAgentsReasoningToolCallsAndDiff() {
        planRepair(0.95, repaired(0.95));

        service.triage(ctx);

        AuditEntry a = audited();
        assertThat(a.id()).isEqualTo(AuditEntry.idFor(ctx.eventId(), AuditEntry.Stage.TRIAGE));
        assertThat(a.eventId()).isEqualTo(ctx.eventId());
        assertThat(a.correlationId()).isEqualTo("cid-1");
        assertThat(a.stage()).isEqualTo(AuditEntry.Stage.TRIAGE);
        assertThat(a.tier()).isEqualTo(AuditEntry.Tier.TIER_1);
        assertThat(a.outcome()).isEqualTo(AuditEntry.Outcome.REPLAYED);
        assertThat(a.actor()).isEqualTo("agent");
        assertThat(a.strategy()).isEqualTo(RemediationStrategy.SCHEMA_DRIFT);
        assertThat(a.plannerConfidence()).isEqualTo(0.95);
        assertThat(a.plannerRationale()).isEqualTo("amount as text");
        assertThat(a.repairSummary()).isEqualTo("amount to number");
        assertThat(a.repairConfidence()).isEqualTo(0.95);
        assertThat(a.toolCalls()).isEqualTo(TOOLS);
        assertThat(a.changes()).isEqualTo(CHANGES);
        assertThat(a.riskFlags()).isEmpty();
        assertThat(a.durationMs()).isNotNegative();
        // Tier 1 is a plain replay plus one insert: no quarantine row, no transaction.
        verifyNoInteractions(repo, txManager);
    }

    @Test
    void tier2AuditRowCarriesTheRiskFlagsAndIsSavedInOneTransactionWithTheHeldRow() {
        planRepair(0.95, repaired(0.95, "createdAt is ambiguous"));

        service.triage(ctx);

        AuditEntry a = audited();
        assertThat(a.tier()).isEqualTo(AuditEntry.Tier.TIER_2);
        assertThat(a.outcome()).isEqualTo(AuditEntry.Outcome.HELD_FOR_APPROVAL);
        assertThat(a.riskFlags()).containsExactly("createdAt is ambiguous");
        assertThat(a.outcomeReason()).startsWith("Awaiting human approval");
        verify(repo).save(any());
        verify(txManager).commit(any()); // quarantine row and audit row commit together
    }

    @Test
    void quarantineAuditRowKeepsTheEvidenceOfAnUnrepairableAttempt() {
        when(planner.plan(ctx)).thenReturn(new RemediationPlan.SchemaRepair("date drift", 0.8));
        when(repairer.repair(ctx)).thenReturn(
                new RepairResult.Unrepairable("gate said no", "tried a date fix", 0.6, TOOLS, CHANGES));

        service.triage(ctx);

        AuditEntry a = audited();
        assertThat(a.outcome()).isEqualTo(AuditEntry.Outcome.QUARANTINED);
        assertThat(a.tier()).isEqualTo(AuditEntry.Tier.NONE);
        assertThat(a.outcomeReason()).isEqualTo("gate said no");
        assertThat(a.plannerRationale()).isEqualTo("date drift");
        assertThat(a.plannerConfidence()).isEqualTo(0.8);
        assertThat(a.repairSummary()).isEqualTo("tried a date fix");
        assertThat(a.repairConfidence()).isEqualTo(0.6);
        assertThat(a.toolCalls()).isEqualTo(TOOLS);
        assertThat(a.changes()).isEqualTo(CHANGES); // what the agent attempted, for debugging prompts
        verify(txManager).commit(any());
    }

    @Test
    void poisonPillAndLoopGuardAreAuditedToo() {
        when(planner.plan(ctx)).thenReturn(new RemediationPlan.Quarantine("corrupt"));
        service.triage(ctx);
        var again = new FailureContext("cid-2", "orders-dlq", 0, 8L, "orders-topic", "k", "{}",
                Map.of("x-repaired-by-agent", "true"), null, Instant.now());
        service.triage(again);

        ArgumentCaptor<AuditEntry> captor = ArgumentCaptor.forClass(AuditEntry.class);
        verify(audit, times(2)).record(captor.capture());
        AuditEntry poison = captor.getAllValues().get(0);
        AuditEntry loop = captor.getAllValues().get(1);
        assertThat(poison.strategy()).isEqualTo(RemediationStrategy.POISON_PILL);
        assertThat(poison.plannerRationale()).isEqualTo("corrupt");
        assertThat(loop.plannerConfidence()).isNull();
        assertThat(loop.plannerRationale()).isNull();
        assertThat(loop.outcomeReason()).contains("already repaired");
    }

    @Test
    void noAuditRowIsWrittenWhenTheReplayFailsSoTheRetryCanDoItProperly() {
        planRepair(0.95, repaired(0.95));
        doThrow(new IllegalStateException("broker down")).when(replayer).replay(any());

        assertThatThrownBy(() -> service.triage(ctx)).isInstanceOf(IllegalStateException.class);

        verifyNoInteractions(audit);
    }
}
