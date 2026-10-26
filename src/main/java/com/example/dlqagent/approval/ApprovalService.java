package com.example.dlqagent.approval;

import com.example.dlqagent.agent.TopicAdapterRegistry;
import com.example.dlqagent.domain.AuditEntry;
import com.example.dlqagent.domain.AuditLog;
import com.example.dlqagent.domain.Evaluation;
import com.example.dlqagent.domain.EventReplayer;
import com.example.dlqagent.domain.QuarantineRecord;
import com.example.dlqagent.domain.QuarantineRepository;
import com.example.dlqagent.domain.QuarantineStatus;
import com.example.dlqagent.domain.ReplayRequest;
import com.example.dlqagent.domain.TopicAdapter;
import com.example.dlqagent.json.JsonDiff;
import com.example.dlqagent.observability.LogSafe;
import com.example.dlqagent.observability.TraceContext;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Human decisions on Tier 2 proposals. Each decision runs in one transaction that holds a row lock, so
 * two reviewers (or one double click) cannot both replay the same event.
 */
@Service
public class ApprovalService {

    private static final Logger log = LoggerFactory.getLogger(ApprovalService.class);
    private static final int MAX_LIMIT = 100;

    private final QuarantineRepository repository;
    private final TopicAdapterRegistry adapters;
    private final EventReplayer replayer;
    private final AuditLog audit;
    private final TraceContext trace;
    private final ObservationRegistry observations;

    ApprovalService(QuarantineRepository repository, TopicAdapterRegistry adapters, EventReplayer replayer,
                    AuditLog audit, TraceContext trace, ObservationRegistry observations) {
        this.repository = repository;
        this.adapters = adapters;
        this.replayer = replayer;
        this.audit = audit;
        this.trace = trace;
        this.observations = observations;
    }

    @Transactional(readOnly = true)
    public List<QuarantineRecord> list(QuarantineStatus status, int limit) {
        return repository.findByStatus(status, Math.max(1, Math.min(limit, MAX_LIMIT)));
    }

    @Transactional(readOnly = true)
    public QuarantineRecord get(UUID id) {
        return repository.findById(id).orElseThrow(() -> new ApprovalNotFoundException(id));
    }

    /**
     * Re-runs the topic's hard checks on the stored proposal, then replays it. Risk flags are not re-litigated:
     * accepting them is exactly what the reviewer is doing. If the replay fails, the transaction rolls back and
     * the event stays pending. If the commit fails after a successful replay the event may be replayed twice on
     * retry (at-least-once), so downstream consumers must be idempotent.
     */
    @Transactional
    public QuarantineRecord approve(UUID id, String reviewer, String notes) {
        return decide("approve", id, reviewer, () -> doApprove(id, reviewer, notes));
    }

    private QuarantineRecord doApprove(UUID id, String reviewer, String notes) {
        long start = System.nanoTime();
        QuarantineRecord rec = lockPending(id);
        TopicAdapter adapter = adapters.forTopic(rec.originalTopic()).orElseThrow(
                () -> new ApprovalConflictException("No topic adapter for '" + rec.originalTopic() + "'"));
        Evaluation evaluation = adapter.evaluate(rec.payload(), rec.proposedPayload(), rec.key());
        if (evaluation.rejected()) {
            throw new ApprovalConflictException(
                    "The proposed payload no longer passes the hard checks: " + String.join("; ", evaluation.violations()));
        }

        try {
            replayer.replay(new ReplayRequest(rec.originalTopic(), rec.key(), rec.proposedPayload(), rec.headers(),
                    rec.correlationId(), rec.repairSummary(), reviewer));
        } catch (RuntimeException e) {
            throw new ReplayFailedException(e);
        }
        repository.markReviewed(id, QuarantineStatus.APPROVED_REPLAYED, reviewer, notes);
        audit.record(decisionEntry(rec, AuditEntry.Outcome.APPROVED_REPLAYED, reviewer, notes, start));
        logDecision("approved and replayed", rec, reviewer);
        return get(id);
    }

    @Transactional
    public QuarantineRecord reject(UUID id, String reviewer, String notes) {
        return decide("reject", id, reviewer, () -> {
            long start = System.nanoTime();
            QuarantineRecord rec = lockPending(id);
            repository.markReviewed(id, QuarantineStatus.REJECTED, reviewer, notes);
            audit.record(decisionEntry(rec, AuditEntry.Outcome.REJECTED, reviewer, notes, start));
            logDecision("rejected", rec, reviewer);
            return get(id);
        });
    }

    /** Runs a decision inside a {@code dlq.approval} span. */
    private QuarantineRecord decide(String action, UUID id, String reviewer, Supplier<QuarantineRecord> body) {
        return Observation.createNotStarted("dlq.approval", observations)
                .lowCardinalityKeyValue("action", action)
                .highCardinalityKeyValue("item.id", id.toString())
                .highCardinalityKeyValue("reviewer", reviewer)
                .observe(body);
    }

    private AuditEntry decisionEntry(QuarantineRecord rec, AuditEntry.Outcome outcome, String reviewer, String notes,
                                     long startNanos) {
        UUID eventId = rec.id(); // the quarantine row id is the DLQ record identity
        String reason = (outcome == AuditEntry.Outcome.REJECTED ? "Rejected" : "Approved") + " by " + reviewer
                + (notes == null || notes.isBlank() ? "" : ": " + notes);
        return new AuditEntry(AuditEntry.idFor(eventId, AuditEntry.Stage.APPROVAL), eventId, rec.correlationId(),
                trace.traceId(), Instant.now(), AuditEntry.Stage.APPROVAL, rec.originalTopic(), rec.key(),
                rec.strategy(), null, null, rec.repairSummary(), null, List.of(),
                JsonDiff.diffOrEmpty(rec.payload(), rec.proposedPayload()), rec.riskFlags(), AuditEntry.Tier.TIER_2,
                outcome, reason, reviewer, (System.nanoTime() - startNanos) / 1_000_000);
    }

    private QuarantineRecord lockPending(UUID id) {
        QuarantineRecord rec = repository.findForUpdate(id).orElseThrow(() -> new ApprovalNotFoundException(id));
        if (rec.status() != QuarantineStatus.PENDING_HUMAN_APPROVAL) {
            throw new ApprovalConflictException("Event is " + rec.status() + ", not " + QuarantineStatus.PENDING_HUMAN_APPROVAL);
        }
        return rec;
    }

    private static void logDecision(String what, QuarantineRecord rec, String reviewer) {
        MDC.put("correlationId", rec.correlationId());
        try {
            log.info("Tier 2 proposal {} id={} reviewer={}", LogSafe.clean(what), LogSafe.clean(rec.id()), LogSafe.clean(reviewer));
        } finally {
            MDC.remove("correlationId");
        }
    }
}
