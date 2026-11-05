package com.example.dlqagent.eval;

import com.example.dlqagent.domain.AuditEntry;
import com.example.dlqagent.domain.AuditLog;
import com.example.dlqagent.domain.EventReplayer;
import com.example.dlqagent.domain.QuarantineRecord;
import com.example.dlqagent.domain.QuarantineRepository;
import com.example.dlqagent.domain.QuarantineStatus;
import com.example.dlqagent.domain.ReplayRequest;
import com.example.dlqagent.domain.RetryEntry;
import com.example.dlqagent.domain.RetryScheduleRepository;
import com.example.dlqagent.domain.RetryStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory stand-ins for the database and the broker, so the evaluation runs the real triage service, planner,
 * repair agent and gate without writing to Postgres or sending to Kafka. Thread-safe; the runner uses several threads.
 */
final class InMemoryStores {

    final Quarantine quarantine = new Quarantine();
    final Audit audit = new Audit();
    final Retries retries = new Retries();
    final Replays replays = new Replays();

    static final class Quarantine implements QuarantineRepository {
        private final Map<UUID, QuarantineRecord> rows = new ConcurrentHashMap<>();

        @Override
        public boolean save(QuarantineRecord r) {
            return rows.putIfAbsent(r.id(), r) == null;
        }

        @Override
        public Optional<QuarantineRecord> findById(UUID id) {
            return Optional.ofNullable(rows.get(id));
        }

        @Override
        public Optional<QuarantineRecord> findForUpdate(UUID id) {
            return findById(id);
        }

        @Override
        public List<QuarantineRecord> findByStatus(QuarantineStatus status, String stream, int limit) {
            return rows.values().stream().filter(r -> r.status() == status).limit(limit).toList();
        }

        @Override
        public boolean markReviewed(UUID id, QuarantineStatus status, String reviewer, String notes) {
            return rows.containsKey(id);
        }
    }

    static final class Audit implements AuditLog {
        private final Map<UUID, AuditEntry> entries = new ConcurrentHashMap<>();

        @Override
        public boolean record(AuditEntry e) {
            return entries.putIfAbsent(e.id(), e) == null;
        }

        @Override
        public List<AuditEntry> findByCorrelationId(String correlationId, int limit) {
            return entries.values().stream().filter(e -> e.correlationId().equals(correlationId)).limit(limit).toList();
        }
    }

    /** Retry rows are kept so the real scheduler runs; the evaluation never replays them (there is no poller). */
    static final class Retries implements RetryScheduleRepository {
        private final Map<String, RetryEntry> byFingerprint = new ConcurrentHashMap<>();

        @Override
        public boolean insertIfAbsent(RetryEntry e) {
            return byFingerprint.putIfAbsent(e.fingerprint(), e) == null;
        }

        @Override
        public Optional<RetryEntry> lockByFingerprint(String fingerprint) {
            return Optional.ofNullable(byFingerprint.get(fingerprint));
        }

        @Override
        public Optional<RetryEntry> lockNextDue(Instant now, String stream, String sourceTopic) {
            return Optional.empty();
        }

        @Override
        public void reschedule(UUID id, Instant nextAttemptAt, UUID lastEventId, String lastError) {
            update(id, e -> with(e, e.attempt(), nextAttemptAt, RetryStatus.SCHEDULED, lastEventId, lastError));
        }

        @Override
        public void markExhausted(UUID id, UUID lastEventId, String lastError) {
            update(id, e -> with(e, e.attempt(), e.nextAttemptAt(), RetryStatus.EXHAUSTED, lastEventId, lastError));
        }

        @Override
        public void markReplayed(UUID id, int attempt) {
            update(id, e -> with(e, attempt, e.nextAttemptAt(), RetryStatus.REPLAYED, e.lastEventId(), e.lastError()));
        }

        private void update(UUID id, java.util.function.UnaryOperator<RetryEntry> change) {
            byFingerprint.replaceAll((fp, e) -> e.id().equals(id) ? change.apply(e) : e);
        }

        private static RetryEntry with(RetryEntry e, int attempt, Instant next, RetryStatus status, UUID eventId, String error) {
            return new RetryEntry(e.id(), e.stream(), e.fingerprint(), e.correlationId(), e.targetTopic(), e.key(),
                    e.payload(), e.headers(), attempt, next, status, eventId, error);
        }
    }

    /** Captures what would have been sent to the broker, by correlation id. */
    static final class Replays implements EventReplayer {
        private final Map<String, ReplayRequest> byCorrelation = new ConcurrentHashMap<>();
        private final List<ReplayRequest> all = java.util.Collections.synchronizedList(new ArrayList<>());

        @Override
        public void replay(ReplayRequest request) {
            byCorrelation.put(request.correlationId(), request);
            all.add(request);
        }

        Optional<ReplayRequest> forCorrelation(String correlationId) {
            return Optional.ofNullable(byCorrelation.get(correlationId));
        }
    }
}
