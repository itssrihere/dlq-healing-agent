package com.example.dlqagent;

import static com.example.dlqagent.support.ScriptedChatModel.decision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.dlqagent.support.MainTopicReader;
import com.example.dlqagent.support.ScriptedChatModel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Strategy A end to end: drifted event on orders-dlq -> planner -> repair agent -> Evaluator Gate ->
 * replay to orders-topic (or quarantine). The LLM is scripted; everything else is real Kafka and Postgres.
 */
class RepairFlowIT extends AbstractIntegrationTest {

    private static final String DEFAULT_REPLY = decision("POISON_PILL", 0.95, "stubbed poison pill");

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired JdbcClient jdbc;
    @Autowired ScriptedChatModel llm;

    @AfterEach
    void resetLlm() {
        llm.respondWith(p -> DEFAULT_REPLY);
    }

    private static String drifted(String orderId) {
        return """
                {"orderId":"%s","customerId":"c-9","amount":"19.99 USD","currency":"USD","status":"PAID","createdAt":"2025-04-03T10:00:00Z"}"""
                .formatted(orderId);
    }

    private void scriptLlm(String repairEdits) {
        llm.respondWith(p -> {
            if (ScriptedChatModel.isPlanner(p)) {
                return decision("SCHEMA_DRIFT", 0.95, "amount carries a currency suffix");
            }
            return "{\"repairable\":true,\"confidence\":0.95,\"summary\":\"amount text to number\",\"edits\":%s,\"dropFields\":[]}"
                    .formatted(repairEdits);
        });
    }

    private void sendToDlq(String key, String payload, Map<String, String> headers) throws Exception {
        var record = new ProducerRecord<>("orders-dlq", key, payload);
        headers.forEach((k, v) -> record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
        kafka.send(record).get();
    }

    private List<ConsumerRecord<String, String>> readMainTopic(String key, Duration atMost) {
        return MainTopicReader.read(KAFKA.getBootstrapServers(), key, atMost);
    }

    private static String header(ConsumerRecord<?, ?> r, String name) {
        return MainTopicReader.header(r, name);
    }

    private int quarantineRows(String cid) {
        return jdbc.sql("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c")
                .param("c", cid).query(Integer.class).single();
    }

    @Test
    void driftedEventIsRepairedAndReplayedWithAuditHeaders() throws Exception {
        scriptLlm("[{\"field\":\"amount\",\"newValue\":\"19.99\"}]");
        String key = "o-" + UUID.randomUUID();
        String cid = "cid-" + UUID.randomUUID();

        sendToDlq(key, drifted(key), Map.of(
                "x-correlation-id", cid,
                "kafka_dlt-original-topic", "orders-topic",
                "tenant", "acme"));

        var replayed = readMainTopic(key, Duration.ofSeconds(30));
        assertThat(replayed).hasSize(1);
        var r = replayed.get(0);
        assertThat(r.value()).contains("\"amount\":19.99").doesNotContain("USD\",\"status\"\"")
                .contains("\"orderId\":\"" + key + "\"");
        assertThat(header(r, "x-repaired-by-agent")).isEqualTo("true");
        assertThat(header(r, "x-correlation-id")).isEqualTo(cid);
        assertThat(header(r, "x-repair-summary")).isEqualTo("amount text to number");
        assertThat(header(r, "tenant")).isEqualTo("acme");
        assertThat(quarantineRows(cid)).isZero();
    }

    @Test
    void patchRejectedByTheGateIsQuarantinedAndNeverReplayed() throws Exception {
        scriptLlm("[{\"field\":\"amount\",\"newValue\":\"99.99\"}]"); // hallucinated: original said 19.99
        String key = "o-" + UUID.randomUUID();
        String cid = "cid-" + UUID.randomUUID();

        sendToDlq(key, drifted(key), Map.of("x-correlation-id", cid, "kafka_dlt-original-topic", "orders-topic"));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(quarantineRows(cid)).isEqualTo(1));
        var row = jdbc.sql("SELECT strategy, reason FROM quarantine_event WHERE correlation_id = :c")
                .param("c", cid).query().singleRow();
        assertThat(row.get("strategy")).isEqualTo("SCHEMA_DRIFT");
        assertThat(row.get("reason").toString()).contains("Evaluator gate rejected", "amount value changed");
        assertThat(readMainTopic(key, Duration.ofSeconds(3))).isEmpty();
    }

    @Test
    void repairedEventThatFailsAgainGoesToQuarantineInsteadOfLooping() throws Exception {
        scriptLlm("[{\"field\":\"amount\",\"newValue\":\"19.99\"}]");
        String key = "o-" + UUID.randomUUID();
        String cid = "cid-" + UUID.randomUUID();

        sendToDlq(key, drifted(key), Map.of(
                "x-correlation-id", cid,
                "kafka_dlt-original-topic", "orders-topic",
                "x-repaired-by-agent", "true"));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(quarantineRows(cid)).isEqualTo(1));
        assertThat(jdbc.sql("SELECT reason FROM quarantine_event WHERE correlation_id = :c")
                .param("c", cid).query(String.class).single()).contains("already repaired");
        assertThat(readMainTopic(key, Duration.ofSeconds(3))).isEmpty();
    }
}
