package com.example.dlqagent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.dlqagent.domain.QuarantineRecord;
import com.example.dlqagent.domain.QuarantineRepository;
import com.example.dlqagent.domain.QuarantineStatus;
import com.example.dlqagent.domain.RemediationStrategy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;

/** Poison event published to orders-dlq ends up quarantined in Postgres (Kafka -> listener -> triage -> JDBC). */
class QuarantineFlowIT extends AbstractIntegrationTest {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired JdbcClient jdbc;
    @Autowired QuarantineRepository repository;

    @Test
    void dlqEventIsQuarantinedWithFailureMetadata() throws Exception {
        String cid = "cid-" + UUID.randomUUID();
        var record = new ProducerRecord<>("orders-dlq", "order-42", "{\"orderId\": \u0000 corrupt");
        record.headers().add("x-correlation-id", cid.getBytes(StandardCharsets.UTF_8));
        record.headers().add("kafka_dlt-original-topic", "orders-topic".getBytes(StandardCharsets.UTF_8));
        record.headers().add("kafka_dlt-exception-stacktrace", "com.example.JsonParseException: boom".getBytes(StandardCharsets.UTF_8));
        record.headers().add("tenant", "acme".getBytes(StandardCharsets.UTF_8));

        kafka.send(record).get();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(jdbc.sql("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c")
                        .param("c", cid).query(Integer.class).single()).isEqualTo(1));

        Map<String, Object> row = jdbc.sql("SELECT * FROM quarantine_event WHERE correlation_id = :c")
                .param("c", cid).query().singleRow();
        assertThat(row.get("original_topic")).isEqualTo("orders-topic");
        assertThat(row.get("payload")).isEqualTo("{\"orderId\": \uFFFD corrupt"); // NUL sanitized
        assertThat(row.get("kafka_key")).isEqualTo("order-42");
        assertThat(row.get("stack_trace")).isEqualTo("com.example.JsonParseException: boom");
        assertThat(row.get("strategy")).isEqualTo("POISON_PILL");
        assertThat(row.get("reason")).isEqualTo("stubbed poison pill"); // came from the planner, via the stub ChatModel
        assertThat(row.get("status")).isEqualTo("PENDING_REVIEW");
        assertThat(row.get("headers").toString()).contains("tenant", "acme");
    }

    @Test
    void savingTheSameRecordTwiceWritesOneRow() {
        var rec = new QuarantineRecord(UUID.randomUUID(), "cid-dup", "orders-topic", null, "p", Map.of(),
                null, RemediationStrategy.POISON_PILL, "r", QuarantineStatus.PENDING_REVIEW, Instant.now(),
                null, null, java.util.List.of(), null, null, null);

        assertThat(repository.save(rec)).isTrue();
        assertThat(repository.save(rec)).isFalse();
        assertThat(jdbc.sql("SELECT count(*) FROM quarantine_event WHERE id = :id")
                .param("id", rec.id()).query(Integer.class).single()).isEqualTo(1);
    }
}
