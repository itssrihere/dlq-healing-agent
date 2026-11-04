package com.example.dlqagent.infrastructure;

import com.example.dlqagent.domain.AuditEntry;
import com.example.dlqagent.domain.AuditLog;
import com.example.dlqagent.domain.FieldChange;
import com.example.dlqagent.domain.RemediationStrategy;
import com.example.dlqagent.domain.ToolCallRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class JdbcAuditLog implements AuditLog {

    private static final String COLUMNS = """
            id, event_id, stream, correlation_id, trace_id, occurred_at, stage, original_topic, kafka_key, strategy,
            planner_confidence, planner_rationale, repair_summary, repair_confidence, tool_calls, changes,
            risk_flags, tier, outcome, outcome_reason, actor, duration_ms""";

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;

    JdbcAuditLog(JdbcClient jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    @Override
    public boolean record(AuditEntry e) {
        int rows = jdbc.sql("""
                INSERT INTO dlq_audit_log
                    (id, event_id, stream, correlation_id, trace_id, occurred_at, stage, original_topic, kafka_key, strategy,
                     planner_confidence, planner_rationale, repair_summary, repair_confidence,
                     tool_calls, changes, risk_flags, tier, outcome, outcome_reason, actor, duration_ms)
                VALUES
                    (:id, :eventId, :stream, :correlationId, :traceId, :occurredAt, :stage, :originalTopic, :key, :strategy,
                     :plannerConfidence, :plannerRationale, :repairSummary, :repairConfidence,
                     CAST(:toolCalls AS jsonb), CAST(:changes AS jsonb), CAST(:riskFlags AS jsonb),
                     :tier, :outcome, :outcomeReason, :actor, :durationMs)
                ON CONFLICT (id) DO NOTHING
                """)
                .param("id", e.id())
                .param("eventId", e.eventId())
                .param("stream", e.stream())
                .param("correlationId", e.correlationId())
                .param("traceId", e.traceId())
                .param("occurredAt", OffsetDateTime.ofInstant(e.occurredAt(), ZoneOffset.UTC))
                .param("stage", e.stage().name())
                .param("originalTopic", e.originalTopic())
                .param("key", PgText.clean(e.key()))
                .param("strategy", e.strategy().name())
                .param("plannerConfidence", e.plannerConfidence())
                .param("plannerRationale", PgText.clean(e.plannerRationale()))
                .param("repairSummary", PgText.clean(e.repairSummary()))
                .param("repairConfidence", e.repairConfidence())
                .param("toolCalls", toJson(e.toolCalls()))
                .param("changes", toJson(PgText.cleanChanges(e.changes())))
                .param("riskFlags", toJson(PgText.cleanAll(e.riskFlags())))
                .param("tier", e.tier().name())
                .param("outcome", e.outcome().name())
                .param("outcomeReason", PgText.clean(e.outcomeReason()))
                .param("actor", PgText.clean(e.actor()))
                .param("durationMs", e.durationMs())
                .update();
        return rows == 1;
    }

    @Override
    public List<AuditEntry> findByCorrelationId(String correlationId, int limit) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM dlq_audit_log WHERE correlation_id = :c "
                        + "ORDER BY occurred_at, id LIMIT :n")
                .param("c", correlationId).param("n", limit).query(this::map).list();
    }

    private AuditEntry map(ResultSet rs, int row) throws SQLException {
        return new AuditEntry(
                rs.getObject("id", UUID.class),
                rs.getObject("event_id", UUID.class),
                rs.getString("stream"),
                rs.getString("correlation_id"),
                rs.getString("trace_id"),
                rs.getObject("occurred_at", OffsetDateTime.class).toInstant(),
                AuditEntry.Stage.valueOf(rs.getString("stage")),
                rs.getString("original_topic"),
                rs.getString("kafka_key"),
                RemediationStrategy.valueOf(rs.getString("strategy")),
                (Double) rs.getObject("planner_confidence"),
                rs.getString("planner_rationale"),
                rs.getString("repair_summary"),
                (Double) rs.getObject("repair_confidence"),
                fromJson(rs.getString("tool_calls"), new TypeReference<List<ToolCallRecord>>() {}),
                fromJson(rs.getString("changes"), new TypeReference<List<FieldChange>>() {}),
                fromJson(rs.getString("risk_flags"), new TypeReference<List<String>>() {}),
                AuditEntry.Tier.valueOf(rs.getString("tier")),
                AuditEntry.Outcome.valueOf(rs.getString("outcome")),
                rs.getString("outcome_reason"),
                rs.getString("actor"),
                rs.getLong("duration_ms"));
    }

    private String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize audit value", e);
        }
    }

    private <T> T fromJson(String json, TypeReference<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Corrupt JSON column in dlq_audit_log", e);
        }
    }
}
