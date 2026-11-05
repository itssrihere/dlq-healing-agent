package com.example.dlqagent.messaging;

import static com.example.dlqagent.support.ScriptedChatModel.decision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.dlqagent.AbstractIntegrationTest;
import com.example.dlqagent.stream.StreamRegistry;
import com.example.dlqagent.support.MainTopicReader;
import com.example.dlqagent.support.ScriptedChatModel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.support.Acknowledgment;

/**
 * "The process died after the work was done but before the offset was committed": Kafka delivers the same DLQ record
 * again. Delivery is at least once by design, so what must hold is that the second pass changes no stored state: one
 * audit row, one quarantine row, one retry row. (A replay to the main topic may be sent twice; that is documented, and
 * the downstream consumer must be idempotent.) The listener is driven directly with a record whose first acknowledgement
 * throws, which is exactly what a crash between triage and commit looks like to the next delivery.
 */
class RedeliveryIT extends AbstractIntegrationTest {

    private static final String DEFAULT_REPLY = decision("POISON_PILL", 0.95, "stubbed poison pill");
    private static final AtomicLong OFFSETS = new AtomicLong(1_000_000);

    @Autowired StreamListeners listeners;
    @Autowired StreamRegistry streams;
    @Autowired ScriptedChatModel llm;
    @Autowired JdbcClient jdbc;

    @AfterEach
    void resetLlm() {
        llm.respondWith(p -> DEFAULT_REPLY);
    }

    private ConsumerRecord<Object, Object> dlqRecord(String key, String payload, String cid) {
        // a fresh offset per test: the DLQ record's identity is (topic, partition, offset), which is what makes writes idempotent
        var record = new ConsumerRecord<Object, Object>("orders-dlq", 0, OFFSETS.incrementAndGet(), key, payload);
        record.headers().add("x-correlation-id", cid.getBytes(StandardCharsets.UTF_8));
        record.headers().add("kafka_dlt-original-topic", "orders-topic".getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private static String textAmountOrder(String id) {
        return "{\"orderId\":\"%s\",\"customerId\":\"c-9\",\"amount\":\"19.99 USD\",\"currency\":\"USD\",\"status\":\"PAID\",\"createdAt\":\"2025-04-03T10:00:00Z\"}"
                .formatted(id);
    }

    private static String validOrder(String id, String createdAt) {
        return "{\"orderId\":\"%s\",\"customerId\":\"c-9\",\"amount\":19.99,\"currency\":\"USD\",\"status\":\"PAID\",\"createdAt\":\"%s\"}"
                .formatted(id, createdAt);
    }

    /** Delivers a record whose acknowledgement fails (the crash), then delivers it again and acknowledges (the redelivery). */
    private AtomicInteger deliverTwice(ConsumerRecord<Object, Object> record) {
        var orders = streams.byName("orders").orElseThrow();
        Acknowledgment crash = new Acknowledgment() {
            @Override
            public void acknowledge() {
                throw new IllegalStateException("process died before the offset was committed");
            }
        };
        assertThatThrownBy(() -> listeners.onRecord(orders, record, crash)).hasMessageContaining("process died");
        AtomicInteger committed = new AtomicInteger();
        listeners.onRecord(orders, record, committed::incrementAndGet);
        return committed;
    }

    private int count(String sql, String cid) {
        return jdbc.sql(sql).param("c", cid).query(Integer.class).single();
    }

    @Test
    void aRedeliveredQuarantineWritesOneRowAndOneAuditEntry() {
        String cid = "cid-" + UUID.randomUUID();

        AtomicInteger committed = deliverTwice(dlqRecord("o-" + UUID.randomUUID(), "{\"orderId\": oops", cid));

        assertThat(committed.get()).as("the offset is committed on the second pass").isEqualTo(1);
        assertThat(count("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c", cid)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM dlq_audit_log WHERE correlation_id = :c", cid)).isEqualTo(1);
    }

    @Test
    void aRedeliveredHeldRepairIsHeldOnceNotTwice() {
        llm.respondWith(p -> ScriptedChatModel.isPlanner(p)
                ? decision("SCHEMA_DRIFT", 0.95, "date in a non ISO format")
                : "{\"repairable\":true,\"confidence\":0.95,\"summary\":\"assumed day first\",\"edits\":"
                        + "[{\"field\":\"createdAt\",\"newValue\":\"2025-04-03T00:00:00Z\"}],\"dropFields\":[]}");
        String cid = "cid-" + UUID.randomUUID();
        String key = "o-" + UUID.randomUUID();

        deliverTwice(dlqRecord(key, validOrder(key, "03/04/2025"), cid));

        assertThat(count("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c AND status = 'PENDING_HUMAN_APPROVAL'", cid))
                .isEqualTo(1);
        assertThat(count("SELECT count(*) FROM dlq_audit_log WHERE correlation_id = :c AND outcome = 'HELD_FOR_APPROVAL'", cid))
                .isEqualTo(1);
    }

    @Test
    void aRedeliveredTransientFailureSchedulesOneRetryAndDoesNotSpendAnotherAttempt() {
        llm.respondWith(p -> decision("TRANSIENT", 0.95, "connection refused"));
        String cid = "cid-" + UUID.randomUUID();
        String key = "o-" + UUID.randomUUID();

        deliverTwice(dlqRecord(key, validOrder(key, "2025-04-03T10:00:00Z"), cid));

        assertThat(count("SELECT count(*) FROM retry_schedule WHERE correlation_id = :c", cid)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM dlq_audit_log WHERE correlation_id = :c AND outcome = 'RETRY_SCHEDULED'", cid)).isEqualTo(1);
        assertThat(jdbc.sql("SELECT attempt FROM retry_schedule WHERE correlation_id = :c").param("c", cid).query(Integer.class).single())
                .as("a redelivery is not a failed retry").isLessThanOrEqualTo(1);
    }

    @Test
    void aRedeliveredAutomaticRepairIsAuditedOnceAndMayBeReplayedTwice() {
        llm.respondWith(p -> ScriptedChatModel.isPlanner(p)
                ? decision("SCHEMA_DRIFT", 0.95, "text amount")
                : "{\"repairable\":true,\"confidence\":0.95,\"summary\":\"text to number\",\"edits\":"
                        + "[{\"field\":\"amount\",\"newValue\":\"19.99\"}],\"dropFields\":[]}");
        String cid = "cid-" + UUID.randomUUID();
        String key = "o-" + UUID.randomUUID();

        deliverTwice(dlqRecord(key, textAmountOrder(key), cid));

        assertThat(count("SELECT count(*) FROM dlq_audit_log WHERE correlation_id = :c AND outcome = 'REPLAYED'", cid))
                .as("one audit row however many deliveries").isEqualTo(1);
        assertThat(count("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c", cid)).isZero();
        // at-least-once: the replay itself is sent on each delivery, which is why the main consumer must be idempotent
        var copies = MainTopicReader.readAll(KAFKA.getBootstrapServers(), key, Duration.ofSeconds(5));
        assertThat(copies).as("the documented duplicate: one replay per delivery").hasSize(2);
        assertThat(copies).extracting(c -> c.value()).allSatisfy(v -> assertThat(v).contains("\"amount\":19.99"));
    }
}
