package com.example.dlqagent.infrastructure;

import com.example.dlqagent.domain.QuarantineRecord;
import com.example.dlqagent.domain.QuarantineRepository;
import com.example.dlqagent.domain.QuarantineStatus;
import com.example.dlqagent.domain.RemediationStrategy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class JdbcQuarantineRepository implements QuarantineRepository {

    private static final String COLUMNS = """
            id, stream, correlation_id, original_topic, kafka_key, payload, headers, stack_trace, strategy, reason, status,
            created_at, proposed_payload, repair_summary, risk_flags, reviewed_by, reviewed_at, review_notes""";

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;

    JdbcQuarantineRepository(JdbcClient jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    @Override
    public boolean save(QuarantineRecord r) {
        int rows = jdbc.sql("""
                INSERT INTO quarantine_event
                    (id, stream, correlation_id, original_topic, kafka_key, payload, headers,
                     stack_trace, strategy, reason, status, created_at,
                     proposed_payload, repair_summary, risk_flags)
                VALUES
                    (:id, :stream, :correlationId, :originalTopic, :key, :payload, CAST(:headers AS jsonb),
                     :stackTrace, :strategy, :reason, :status, :createdAt,
                     :proposedPayload, :repairSummary, CAST(:riskFlags AS jsonb))
                ON CONFLICT (id) DO NOTHING
                """)
                .param("id", r.id())
                .param("stream", r.stream())
                .param("correlationId", r.correlationId())
                .param("originalTopic", r.originalTopic())
                .param("key", PgText.clean(r.key()))
                .param("payload", PgText.clean(r.payload()))
                .param("headers", toJson(PgText.cleanHeaders(r.headers())))
                .param("stackTrace", PgText.clean(r.stackTrace()))
                .param("strategy", r.strategy().name())
                .param("reason", PgText.clean(r.reason()))
                .param("status", r.status().name())
                .param("createdAt", OffsetDateTime.ofInstant(r.createdAt(), ZoneOffset.UTC))
                .param("proposedPayload", PgText.clean(r.proposedPayload()))
                .param("repairSummary", PgText.clean(r.repairSummary()))
                .param("riskFlags", toJson(PgText.cleanAll(r.riskFlags())))
                .update();
        return rows == 1;
    }

    @Override
    public Optional<QuarantineRecord> findById(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM quarantine_event WHERE id = :id")
                .param("id", id).query(this::map).optional();
    }

    @Override
    public Optional<QuarantineRecord> findForUpdate(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM quarantine_event WHERE id = :id FOR UPDATE")
                .param("id", id).query(this::map).optional();
    }

    @Override
    public List<QuarantineRecord> findByStatus(QuarantineStatus status, String stream, int limit) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM quarantine_event WHERE status = :s "
                        + "AND (CAST(:stream AS TEXT) IS NULL OR stream = :stream) ORDER BY created_at DESC LIMIT :n")
                .param("s", status.name()).param("stream", stream).param("n", limit).query(this::map).list();
    }

    @Override
    public boolean markReviewed(UUID id, QuarantineStatus status, String reviewer, String notes) {
        return jdbc.sql("""
                UPDATE quarantine_event
                SET status = :status, reviewed_by = :reviewer, reviewed_at = now(), review_notes = :notes
                WHERE id = :id
                """)
                .param("status", status.name()).param("reviewer", PgText.clean(reviewer))
                .param("notes", PgText.clean(notes)).param("id", id).update() == 1;
    }

    private QuarantineRecord map(ResultSet rs, int row) throws SQLException {
        OffsetDateTime reviewedAt = rs.getObject("reviewed_at", OffsetDateTime.class);
        return new QuarantineRecord(
                rs.getObject("id", UUID.class),
                rs.getString("stream"),
                rs.getString("correlation_id"),
                rs.getString("original_topic"),
                rs.getString("kafka_key"),
                rs.getString("payload"),
                fromJson(rs.getString("headers"), new TypeReference<Map<String, String>>() {}),
                rs.getString("stack_trace"),
                RemediationStrategy.valueOf(rs.getString("strategy")),
                rs.getString("reason"),
                QuarantineStatus.valueOf(rs.getString("status")),
                rs.getObject("created_at", OffsetDateTime.class).toInstant(),
                rs.getString("proposed_payload"),
                rs.getString("repair_summary"),
                fromJson(rs.getString("risk_flags"), new TypeReference<List<String>>() {}),
                rs.getString("reviewed_by"),
                reviewedAt == null ? null : reviewedAt.toInstant(),
                rs.getString("review_notes"));
    }

    private String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize value for quarantine", e);
        }
    }

    private <T> T fromJson(String json, TypeReference<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Corrupt JSON column in quarantine_event", e);
        }
    }
}
