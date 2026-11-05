package com.example.dlqagent;

import static com.example.dlqagent.support.ScriptedChatModel.decision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.dlqagent.support.MainTopicReader;
import com.example.dlqagent.support.ScriptedChatModel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

/**
 * What happens when the infrastructure under the agent misbehaves, against the real stack. Postgres and the broker are
 * frozen (docker pause) mid-flow, which is what a stalled disk, a network partition or a failover looks like from the
 * application: calls hang rather than fail. In every case the event must end up processed, never lost, and the Kafka
 * offset must really be committed afterwards. Duplicated replays are allowed (at-least-once); duplicated state is not.
 */
class ResilienceIT extends AbstractIntegrationTest {

    private static final String DEFAULT_REPLY = decision("POISON_PILL", 0.95, "stubbed poison pill");

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired JdbcClient jdbc;
    @Autowired ScriptedChatModel llm;

    @AfterEach
    void resetLlm() {
        llm.respondWith(p -> DEFAULT_REPLY);
    }

    // ---- helpers ---------------------------------------------------------------------------------------------

    private static void pause(GenericContainer<?> container) {
        DockerClientFactory.instance().client().pauseContainerCmd(container.getContainerId()).exec();
    }

    private static void unpause(GenericContainer<?> container) {
        try {
            DockerClientFactory.instance().client().unpauseContainerCmd(container.getContainerId()).exec();
        } catch (RuntimeException alreadyRunning) {
            // not paused: nothing to undo
        }
    }

    private void sendToDlq(String key, String payload, String cid, Map<String, String> extraHeaders) throws Exception {
        var record = new ProducerRecord<>("orders-dlq", key, payload);
        record.headers().add("x-correlation-id", cid.getBytes(StandardCharsets.UTF_8));
        record.headers().add("kafka_dlt-original-topic", "orders-topic".getBytes(StandardCharsets.UTF_8));
        extraHeaders.forEach((k, v) -> record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
        kafka.send(record).get(30, TimeUnit.SECONDS);
    }

    private static String order(String id, String amount) {
        return "{\"orderId\":\"%s\",\"customerId\":\"c-9\",\"amount\":%s,\"currency\":\"USD\",\"status\":\"PAID\",\"createdAt\":\"2025-04-03T10:00:00Z\"}"
                .formatted(id, amount);
    }

    private int count(String sql, String cid) {
        return jdbc.sql(sql).param("c", cid).query(Integer.class).single();
    }

    /** The DLQ's consumer group has committed everything on orders-dlq: the offset really moved on. */
    private void awaitOrdersDlqFullyCommitted() {
        try (AdminClient admin = AdminClient.create(new HashMap<String, Object>(kafkaAdmin.getConfigurationProperties()))) {
            await().atMost(Duration.ofSeconds(90)).untilAsserted(() -> {
                var committed = admin.listConsumerGroupOffsets("dlq-healing-agent").partitionsToOffsetAndMetadata().get(15, TimeUnit.SECONDS);
                Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
                for (int p = 0; p < 3; p++) {
                    latest.put(new TopicPartition("orders-dlq", p), OffsetSpec.latest());
                }
                var ends = admin.listOffsets(latest).all().get(15, TimeUnit.SECONDS);
                for (var tp : latest.keySet()) {
                    long done = committed.containsKey(tp) ? committed.get(tp).offset() : 0;
                    assertThat(done).as("committed offset of " + tp).isEqualTo(ends.get(tp).offset());
                }
            });
        }
    }

    private void scriptTier1Repair(CountDownLatch repairStarted, CountDownLatch release, AtomicInteger repairCalls) {
        llm.respondWith(p -> {
            if (ScriptedChatModel.isPlanner(p)) {
                return decision("SCHEMA_DRIFT", 0.95, "text amount");
            }
            repairCalls.incrementAndGet();
            repairStarted.countDown();
            try {
                release.await(60, TimeUnit.SECONDS); // the test freezes the broker while the model is "thinking"
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "{\"repairable\":true,\"confidence\":0.95,\"summary\":\"text to number\",\"edits\":"
                    + "[{\"field\":\"amount\",\"newValue\":\"19.99\"}],\"dropFields\":[]}";
        });
    }

    // ---- tests -----------------------------------------------------------------------------------------------

    @Test
    void postgresFrozenMidTriageDelaysTheEventButNeverLosesOrDuplicatesIt() throws Exception {
        String cid = "cid-" + UUID.randomUUID();
        String key = "o-" + UUID.randomUUID();
        pause(POSTGRES);
        try {
            sendToDlq(key, "{\"orderId\": corrupt", cid, Map.of());
            Thread.sleep(4_000); // the listener is now stuck on its first database write
        } finally {
            unpause(POSTGRES);
        }

        await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c", cid)).isEqualTo(1));
        awaitOrdersDlqFullyCommitted();
        Thread.sleep(2_000);
        assertThat(count("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c", cid)).as("still exactly one").isEqualTo(1);
        assertThat(count("SELECT count(*) FROM dlq_audit_log WHERE correlation_id = :c", cid)).isEqualTo(1);
    }

    @Test
    void aShortBrokerStallDuringReplayIsSurvived() throws Exception {
        CountDownLatch repairStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger repairCalls = new AtomicInteger();
        scriptTier1Repair(repairStarted, release, repairCalls);
        String cid = "cid-" + UUID.randomUUID();
        String key = "o-" + UUID.randomUUID();
        sendToDlq(key, order(key, "\"19.99 USD\""), cid, Map.of());
        assertThat(repairStarted.await(30, TimeUnit.SECONDS)).isTrue();

        pause(KAFKA);
        try {
            release.countDown(); // the repair now finishes and the replay hits a frozen broker
            Thread.sleep(3_000);  // shorter than the 10 s the replay waits for the broker
        } finally {
            unpause(KAFKA);
        }

        var copies = MainTopicReader.read(KAFKA.getBootstrapServers(), key, Duration.ofSeconds(60));
        assertThat(copies).isNotEmpty();
        assertThat(copies.get(0).value()).contains("\"amount\":19.99");
        assertThat(repairCalls.get()).as("the stall was shorter than the replay's wait, so the record was handled once").isEqualTo(1);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM dlq_audit_log WHERE correlation_id = :c AND outcome = 'REPLAYED'", cid)).isEqualTo(1));
        assertThat(count("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c", cid)).isZero();
        awaitOrdersDlqFullyCommitted();
    }

    @Test
    void aLongBrokerOutageFailsTheDeliveryAndTheEventIsRedeliveredNotLost() throws Exception {
        CountDownLatch repairStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger repairCalls = new AtomicInteger();
        scriptTier1Repair(repairStarted, release, repairCalls);
        String cid = "cid-" + UUID.randomUUID();
        String key = "o-" + UUID.randomUUID();
        sendToDlq(key, order(key, "\"19.99 USD\""), cid, Map.of());
        assertThat(repairStarted.await(30, TimeUnit.SECONDS)).isTrue();

        pause(KAFKA);
        try {
            release.countDown();
            Thread.sleep(14_000); // longer than the replay's 10 s wait: the delivery fails and Kafka must hand it over again
        } finally {
            unpause(KAFKA);
        }

        await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM dlq_audit_log WHERE correlation_id = :c AND outcome = 'REPLAYED'", cid)).isEqualTo(1));
        var copies = MainTopicReader.readAll(KAFKA.getBootstrapServers(), key, Duration.ofSeconds(8));
        assertThat(copies.size()).as("at least once: a copy always arrives, a duplicate is allowed").isBetween(1, 2);
        assertThat(copies).allSatisfy(c -> assertThat(c.value()).contains("\"amount\":19.99"));
        // The proof that the delivery really failed and was redone: the model was asked a second time. The replayed copy
        // alone cannot show it, because a record the producer had already buffered reaches the broker once it is back.
        assertThat(repairCalls.get()).as("the failed delivery was handed over again").isGreaterThanOrEqualTo(2);
        assertThat(count("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c", cid)).isZero();
        awaitOrdersDlqFullyCommitted();
    }

    @Test
    void aFailingModelQuarantinesThatEventAndTheNextOneIsStillHealed() throws Exception {
        AtomicInteger plannerCalls = new AtomicInteger();
        llm.respondWith(p -> {
            if (ScriptedChatModel.isPlanner(p)) {
                if (plannerCalls.incrementAndGet() == 1) {
                    throw new IllegalStateException("model endpoint down");
                }
                return decision("SCHEMA_DRIFT", 0.95, "text amount");
            }
            return "{\"repairable\":true,\"confidence\":0.95,\"summary\":\"text to number\",\"edits\":"
                    + "[{\"field\":\"amount\",\"newValue\":\"19.99\"}],\"dropFields\":[]}";
        });
        String firstKey = "o-" + UUID.randomUUID();
        String firstCid = "cid-" + UUID.randomUUID();
        sendToDlq(firstKey, order(firstKey, "\"19.99 USD\""), firstCid, Map.of());
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c", firstCid)).isEqualTo(1));

        String secondKey = "o-" + UUID.randomUUID();
        String secondCid = "cid-" + UUID.randomUUID();
        // the same amount as the scripted repair writes: a repair that changed the value would rightly be rejected by the gate
        sendToDlq(secondKey, order(secondKey, "\"19.99 USD\""), secondCid, Map.of());

        assertThat(jdbc.sql("SELECT status FROM quarantine_event WHERE correlation_id = :c").param("c", firstCid)
                .query(String.class).single()).isEqualTo("PENDING_REVIEW"); // failed closed, for a human
        var healed = MainTopicReader.read(KAFKA.getBootstrapServers(), secondKey, Duration.ofSeconds(30));
        assertThat(healed).hasSize(1);
        assertThat(count("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c", secondCid)).isZero();
    }

    @Test
    void anEventTheAgentAlreadyRepairedIsNeverRepairedAgain() throws Exception {
        String key = "o-" + UUID.randomUUID();
        String cid = "cid-" + UUID.randomUUID();
        int promptsBefore = llm.prompts().size();

        sendToDlq(key, order(key, "19.99"), cid, Map.of("x-repaired-by-agent", "true"));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c", cid)).isEqualTo(1));
        assertThat(jdbc.sql("SELECT reason FROM quarantine_event WHERE correlation_id = :c").param("c", cid)
                .query(String.class).single()).contains("already repaired");
        assertThat(llm.prompts().size()).as("the model was never asked").isEqualTo(promptsBefore);
        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), key, Duration.ofSeconds(3))).isEmpty();
    }
}
