package com.example.dlqagent.infrastructure;

import com.example.dlqagent.domain.RetryEntry;
import com.example.dlqagent.domain.RetryScheduleRepository;
import com.example.dlqagent.domain.RetryStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class JdbcRetryScheduleRepository implements RetryScheduleRepository {

    private static final String COLUMNS = """
            id, fingerprint, correlation_id, target_topic, kafka_key, payload, headers, attempt, next_attempt_at,
            status, last_event_id, last_error""";

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;

    JdbcRetryScheduleRepository(JdbcClient jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    @Override
    public boolean insertIfAbsent(RetryEntry e) {
        return jdbc.sql("""
                INSERT INTO retry_schedule
                    (id, fingerprint, correlation_id, target_topic, kafka_key, payload, headers, attempt,
                     next_attempt_at, status, last_event_id, last_error)
                VALUES
                    (:id, :fingerprint, :correlationId, :topic, :key, :payload, CAST(:headers AS jsonb), :attempt,
                     :next, :status, :lastEventId, :lastError)
                ON CONFLICT (fingerprint) DO NOTHING
                """)
                .param("id", e.id())
                .param("fingerprint", e.fingerprint())
                .param("correlationId", e.correlationId())
                .param("topic", e.targetTopic())
                .param("key", PgText.clean(e.key()))
                .param("payload", PgText.clean(e.payload()))
                .param("headers", toJson(PgText.cleanHeaders(e.headers())))
                .param("attempt", e.attempt())
                .param("next", at(e.nextAttemptAt()))
                .param("status", e.status().name())
                .param("lastEventId", e.lastEventId())
                .param("lastError", PgText.clean(e.lastError()))
                .update() == 1;
    }

    @Override
    public Optional<RetryEntry> lockByFingerprint(String fingerprint) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM retry_schedule WHERE fingerprint = :f FOR UPDATE")
                .param("f", fingerprint).query(this::map).optional();
    }

    @Override
    public Optional<RetryEntry> lockNextDue(Instant now) {
        return jdbc.sql("SELECT " + COLUMNS + """
                 FROM retry_schedule
                WHERE status = 'SCHEDULED' AND next_attempt_at <= :now
                ORDER BY next_attempt_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """)
                .param("now", at(now)).query(this::map).optional();
    }

    @Override
    public void reschedule(UUID id, Instant nextAttemptAt, UUID lastEventId, String lastError) {
        jdbc.sql("""
                UPDATE retry_schedule
                SET status = 'SCHEDULED', next_attempt_at = :next, last_event_id = :event, last_error = :error,
                    updated_at = now()
                WHERE id = :id
                """)
                .param("next", at(nextAttemptAt)).param("event", lastEventId)
                .param("error", PgText.clean(lastError)).param("id", id).update();
    }

    @Override
    public void markExhausted(UUID id, UUID lastEventId, String lastError) {
        jdbc.sql("""
                UPDATE retry_schedule
                SET status = 'EXHAUSTED', last_event_id = :event, last_error = :error, updated_at = now()
                WHERE id = :id
                """)
                .param("event", lastEventId).param("error", PgText.clean(lastError)).param("id", id).update();
    }

    @Override
    public void markReplayed(UUID id, int attempt) {
        jdbc.sql("UPDATE retry_schedule SET status = 'REPLAYED', attempt = :attempt, updated_at = now() WHERE id = :id")
                .param("attempt", attempt).param("id", id).update();
    }

    private RetryEntry map(ResultSet rs, int row) throws SQLException {
        return new RetryEntry(
                rs.getObject("id", UUID.class),
                rs.getString("fingerprint"),
                rs.getString("correlation_id"),
                rs.getString("target_topic"),
                rs.getString("kafka_key"),
                rs.getString("payload"),
                fromJson(rs.getString("headers")),
                rs.getInt("attempt"),
                rs.getObject("next_attempt_at", OffsetDateTime.class).toInstant(),
                RetryStatus.valueOf(rs.getString("status")),
                rs.getObject("last_event_id", UUID.class),
                rs.getString("last_error"));
    }

    private static OffsetDateTime at(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize value for retry_schedule", e);
        }
    }

    private Map<String, String> fromJson(String json) {
        try {
            return mapper.readValue(json, new TypeReference<Map<String, String>>() {});
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Corrupt JSON column in retry_schedule", e);
        }
    }
}
