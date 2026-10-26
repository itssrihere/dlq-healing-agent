package com.example.dlqagent.agent;

import com.example.dlqagent.domain.AuditEntry;
import com.example.dlqagent.domain.AuditLog;
import com.example.dlqagent.domain.EventReplayer;
import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.domain.FieldChange;
import com.example.dlqagent.domain.PayloadRepairer;
import com.example.dlqagent.domain.QuarantineRecord;
import com.example.dlqagent.domain.QuarantineRepository;
import com.example.dlqagent.domain.RemediationPlan;
import com.example.dlqagent.domain.RemediationPlanner;
import com.example.dlqagent.domain.RemediationStrategy;
import com.example.dlqagent.domain.RepairResult;
import com.example.dlqagent.domain.ReplayRequest;
import com.example.dlqagent.domain.ToolCallRecord;
import com.example.dlqagent.infrastructure.DlqAgentProperties;
import com.example.dlqagent.observability.TraceContext;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Orchestrates one failed event: ask the planner, then execute the chosen strategy.
 *
 * <p>A repair that passes the hard checks is routed by a deterministic two-tier policy: Tier 1 replays
 * automatically; Tier 2 persists the proposal for a human. Every logical failure (planner, repair, hard
 * violation) fails closed to quarantine. Infrastructure failures (database or broker down) propagate so the
 * listener retries instead of losing the event.
 *
 * <p>Every outcome writes one audit row. Quarantine and Tier 2 rows are saved in the same transaction as their
 * audit row, so a held event can never lack an audit record. The whole run is a trace span ({@code dlq.triage}),
 * with child spans for planning, repair and replay.
 */
@Service
public class DlqTriageService {

    private static final Logger log = LoggerFactory.getLogger(DlqTriageService.class);
    private static final String AGENT = "agent";

    private final RemediationPlanner planner;
    private final PayloadRepairer repairer;
    private final EventReplayer replayer;
    private final QuarantineRepository quarantine;
    private final AuditLog audit;
    private final ObservationRegistry observations;
    private final TransactionTemplate tx;
    private final TraceContext trace;
    private final String mainTopic;
    private final double autoReplayMinConfidence;

    public DlqTriageService(RemediationPlanner planner, PayloadRepairer repairer, EventReplayer replayer,
                            QuarantineRepository quarantine, AuditLog audit, DlqAgentProperties props,
                            ObservationRegistry observations, TransactionTemplate tx, TraceContext trace) {
        this.planner = planner;
        this.repairer = repairer;
        this.replayer = replayer;
        this.quarantine = quarantine;
        this.audit = audit;
        this.observations = observations;
        this.tx = tx;
        this.trace = trace;
        this.mainTopic = props.topics().main();
        this.autoReplayMinConfidence = props.gate().autoReplayMinConfidence();
    }

    /** What has been learned about one event so far; becomes its audit row. */
    private static final class Draft {
        final long startNanos = System.nanoTime();
        Double plannerConfidence;
        String plannerRationale;
        String repairSummary;
        Double repairConfidence;
        List<ToolCallRecord> toolCalls = List.of();
        List<FieldChange> changes = List.of();
        AuditEntry.Outcome outcome;
        AuditEntry.Tier tier;
    }

    public void triage(FailureContext ctx) {
        Draft draft = new Draft();
        Observation span = Observation.createNotStarted("dlq.triage", observations)
                .lowCardinalityKeyValue("topic", ctx.originalTopic())
                .highCardinalityKeyValue("correlation.id", ctx.correlationId())
                .highCardinalityKeyValue("event.id", ctx.eventId().toString())
                .start();
        try (Observation.Scope ignored = span.openScope()) {
            run(ctx, draft);
            span.lowCardinalityKeyValue("outcome", String.valueOf(draft.outcome));
            span.lowCardinalityKeyValue("tier", String.valueOf(draft.tier));
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.stop();
        }
    }

    private void run(FailureContext ctx, Draft d) {
        if (ctx.alreadyRepaired()) {
            // Loop guard: a repaired event that fails again must go to a human, not around the loop.
            quarantine(ctx, d, RemediationStrategy.POISON_PILL, "Event was already repaired by the agent and failed again");
            return;
        }

        switch (planSafely(ctx)) {
            case RemediationPlan.Quarantine q -> {
                d.plannerRationale = q.reason();
                quarantine(ctx, d, RemediationStrategy.POISON_PILL, q.reason());
            }
            case RemediationPlan.SchemaRepair s -> {
                d.plannerRationale = s.rationale();
                d.plannerConfidence = s.confidence();
                repair(ctx, d);
            }
            // Strategy B arrives in a later phase; until then it fails closed.
            case RemediationPlan.TransientRetry t -> {
                d.plannerRationale = t.rationale();
                d.plannerConfidence = t.confidence();
                quarantine(ctx, d, RemediationStrategy.TRANSIENT, "Transient retry is not implemented yet: " + t.rationale());
            }
        }
    }

    private void repair(FailureContext ctx, Draft d) {
        if (!mainTopic.equals(ctx.originalTopic())) {
            // The original-topic header is data from the failed event; only replay to the topic we serve.
            quarantine(ctx, d, RemediationStrategy.SCHEMA_DRIFT,
                    "Refusing to replay to unexpected topic '" + ctx.originalTopic() + "'");
            return;
        }
        switch (repairSafely(ctx)) {
            case RepairResult.Unrepairable u -> {
                d.repairSummary = u.summary();
                d.repairConfidence = u.confidence();
                d.toolCalls = u.toolCalls();
                d.changes = u.attempted();
                quarantine(ctx, d, RemediationStrategy.SCHEMA_DRIFT, u.reason());
            }
            case RepairResult.Repaired r -> {
                d.repairSummary = r.summary();
                d.repairConfidence = r.confidence();
                d.toolCalls = r.toolCalls();
                d.changes = r.changes();
                List<String> why = tier2Reasons(r);
                if (why.isEmpty()) {
                    replayer.replay(ReplayRequest.automatic(ctx, r));
                    record(ctx, d, AuditEntry.Tier.TIER_1, AuditEntry.Outcome.REPLAYED, r.summary(), List.of());
                    log.info("Tier 1: repaired and replayed event topic={} confidence={} summary={}",
                            ctx.originalTopic(), r.confidence(), r.summary());
                } else {
                    String reason = "Awaiting human approval: " + String.join("; ", why);
                    QuarantineRecord held = QuarantineRecord.awaitingApproval(ctx, r, reason);
                    AuditEntry entry = entry(ctx, d, RemediationStrategy.SCHEMA_DRIFT, AuditEntry.Tier.TIER_2,
                            AuditEntry.Outcome.HELD_FOR_APPROVAL, reason, r.riskFlags());
                    boolean written = saveAtomically(held, entry);
                    log.info("Tier 2: repair held for approval newRow={} reasons={}", written, why);
                }
            }
        }
    }

    /**
     * The tier policy, deterministic. Tier 1 needs no risk flags and enough confidence in the repair itself.
     * Confidence can only demote: a model saying "very sure" never overrides a risk flag.
     *
     * <p>The planner's confidence is deliberately not used here. It measures "is this schema drift", which the
     * planner already had to clear {@code planner.min-confidence} for; it says nothing about whether this specific
     * patch is safe, which the gate and the repair confidence judge. (Mixing them sent the clean text-amount fix to
     * Tier 2 in a real run, because the real planner reported 0.80 for it.)
     */
    private List<String> tier2Reasons(RepairResult.Repaired repaired) {
        List<String> why = new ArrayList<>(repaired.riskFlags());
        if (repaired.confidence() < autoReplayMinConfidence) {
            why.add("repair confidence %.2f is below the auto-replay threshold %.2f"
                    .formatted(repaired.confidence(), autoReplayMinConfidence));
        }
        return why;
    }

    private RemediationPlan planSafely(FailureContext ctx) {
        try {
            RemediationPlan plan = step("dlq.plan", ctx, () -> planner.plan(ctx));
            return plan != null ? plan : new RemediationPlan.Quarantine("Planner returned no plan");
        } catch (RuntimeException e) {
            log.warn("Planner failed, quarantining event", e);
            return new RemediationPlan.Quarantine("Planner failed: " + e.getClass().getSimpleName());
        }
    }

    private RepairResult repairSafely(FailureContext ctx) {
        try {
            RepairResult result = step("dlq.repair", ctx, () -> repairer.repair(ctx));
            return result != null ? result : new RepairResult.Unrepairable("Repairer returned no result");
        } catch (RuntimeException e) {
            log.warn("Repairer failed, quarantining event", e);
            return new RepairResult.Unrepairable("Repairer failed: " + e.getClass().getSimpleName());
        }
    }

    private <T> T step(String name, FailureContext ctx, Supplier<T> body) {
        return Observation.createNotStarted(name, observations)
                .highCardinalityKeyValue("correlation.id", ctx.correlationId())
                .observe(body);
    }

    private void quarantine(FailureContext ctx, Draft d, RemediationStrategy strategy, String reason) {
        QuarantineRecord held = QuarantineRecord.pending(ctx, strategy, reason);
        AuditEntry entry = entry(ctx, d, strategy, AuditEntry.Tier.NONE, AuditEntry.Outcome.QUARANTINED, reason, List.of());
        boolean written = saveAtomically(held, entry);
        log.info("Quarantined event strategy={} newRow={} originalTopic={} reason={}",
                strategy, written, ctx.originalTopic(), reason);
    }

    /** Quarantine row and its audit row commit together or not at all. */
    private boolean saveAtomically(QuarantineRecord held, AuditEntry entry) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            boolean written = quarantine.save(held);
            audit.record(entry);
            return written;
        }));
    }

    private void record(FailureContext ctx, Draft d, AuditEntry.Tier tier, AuditEntry.Outcome outcome,
                        String reason, List<String> flags) {
        audit.record(entry(ctx, d, RemediationStrategy.SCHEMA_DRIFT, tier, outcome, reason, flags));
    }

    private AuditEntry entry(FailureContext ctx, Draft d, RemediationStrategy strategy, AuditEntry.Tier tier,
                             AuditEntry.Outcome outcome, String reason, List<String> flags) {
        d.tier = tier;
        d.outcome = outcome;
        return new AuditEntry(
                AuditEntry.idFor(ctx.eventId(), AuditEntry.Stage.TRIAGE), ctx.eventId(), ctx.correlationId(),
                trace.traceId(), Instant.now(), AuditEntry.Stage.TRIAGE, ctx.originalTopic(), ctx.key(), strategy,
                d.plannerConfidence, d.plannerRationale, d.repairSummary, d.repairConfidence, d.toolCalls, d.changes,
                flags, tier, outcome, reason, AGENT, (System.nanoTime() - d.startNanos) / 1_000_000);
    }
}
