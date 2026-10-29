package com.example.dlqagent;

import static com.example.dlqagent.support.ScriptedChatModel.decision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.dlqagent.retry.ReplayCircuitBreaker;
import com.example.dlqagent.retry.RetryPoller;
import com.example.dlqagent.support.MainTopicReader;
import com.example.dlqagent.support.ScriptedChatModel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Strategy B end to end with real Kafka and Postgres and a scripted model: a transient failure on orders-dlq is
 * scheduled, replayed unchanged after the backoff, and either left alone (it worked) or rescheduled, and finally
 * quarantined when it keeps coming back. Test timings are in {@link AbstractIntegrationTest}.
 */
class TransientRetryFlowIT extends AbstractIntegrationTest {

    private static final String DEFAULT_REPLY = decision("POISON_PILL", 0.95, "stubbed poison pill");

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired JdbcClient jdbc;
    @Autowired ScriptedChatModel llm;
    @Autowired RetryPoller poller;
    @Autowired ReplayCircuitBreaker breaker;

    @AfterEach
    void resetLlm() {
        llm.respondWith(p -> DEFAULT_REPLY);
    }

    private void planTransient() {
        llm.respondWith(p -> decision("TRANSIENT", 0.95, "connection reset by peer"));
    }

    private static String payload(String orderId) {
        return "{\"orderId\":\"%s\",\"customerId\":\"c-9\",\"amount\":19.99,\"currency\":\"USD\",\"status\":\"PAID\",\"createdAt\":\"2025-04-03T10:00:00Z\"}"
                .formatted(orderId);
    }

    private void sendToDlq(String key, String payload, String cid) throws Exception {
        var record = new ProducerRecord<>("orders-dlq", key, payload);
        record.headers().add("x-correlation-id", cid.getBytes(StandardCharsets.UTF_8));
        record.headers().add("kafka_dlt-original-topic", "orders-topic".getBytes(StandardCharsets.UTF_8));
        record.headers().add("kafka_dlt-exception-stacktrace",
                "java.net.ConnectException: Connection refused".getBytes(StandardCharsets.UTF_8));
        record.headers().add("tenant", "acme".getBytes(StandardCharsets.UTF_8));
        kafka.send(record).get();
    }

    private Integer scalar(String sql, String cid) {
        return jdbc.sql(sql).param("c", cid).query(Integer.class).single();
    }

    private int auditRows(String cid, String outcome) {
        return jdbc.sql("SELECT count(*) FROM dlq_audit_log WHERE correlation_id = :c AND outcome = :o")
                .param("c", cid).param("o", outcome).query(Integer.class).single();
    }

    private String retryStatus(String cid) {
        return jdbc.sql("SELECT status FROM retry_schedule WHERE correlation_id = :c")
                .param("c", cid).query(String.class).optional().orElse("(none)");
    }

    @Test
    void transientFailureIsReplayedUnchangedAfterTheBackoffAndAudited() throws Exception {
        planTransient();
        String key = "o-" + UUID.randomUUID();
        String cid = "cid-" + UUID.randomUUID();
        String original = payload(key);

        sendToDlq(key, original, cid);

        var replayed = MainTopicReader.read(KAFKA.getBootstrapServers(), key, Duration.ofSeconds(30));
        assertThat(replayed).hasSize(1);
        var r = replayed.get(0);
        assertThat(r.value()).isEqualTo(original); // byte for byte: Strategy B never edits data
        assertThat(MainTopicReader.header(r, "x-retry-attempt")).isEqualTo("1");
        assertThat(MainTopicReader.header(r, "x-repaired-by-agent")).isNull(); // not a repair
        assertThat(MainTopicReader.header(r, "x-correlation-id")).isEqualTo(cid);
        assertThat(MainTopicReader.header(r, "tenant")).isEqualTo("acme"); // the failed event's own headers survive

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(retryStatus(cid)).isEqualTo("REPLAYED"));
        assertThat(scalar("SELECT attempt FROM retry_schedule WHERE correlation_id = :c", cid)).isEqualTo(1);
        assertThat(auditRows(cid, "RETRY_SCHEDULED")).isEqualTo(1);
        assertThat(auditRows(cid, "RETRY_REPLAYED")).isEqualTo(1);
        assertThat(scalar("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c", cid)).isZero();
        assertThat(jdbc.sql("SELECT stage FROM dlq_audit_log WHERE correlation_id = :c AND outcome = 'RETRY_REPLAYED'")
                .param("c", cid).query(String.class).single()).isEqualTo("RETRY");
    }

    @Test
    void anEventThatKeepsFailingIsRetriedWithinItsBudgetThenQuarantined() throws Exception {
        planTransient();
        String key = "o-" + UUID.randomUUID();
        String cid = "cid-" + UUID.randomUUID();
        String original = payload(key);

        sendToDlq(key, original, cid); // the first failure
        for (int replay = 1; replay <= 3; replay++) { // retry.max-attempts = 3 in tests
            int expected = replay;
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(retryStatus(cid)).isEqualTo("REPLAYED");
                assertThat(scalar("SELECT attempt FROM retry_schedule WHERE correlation_id = :c", cid)).isEqualTo(expected);
            });
            sendToDlq(key, original, cid); // the replay failed downstream and came back
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(retryStatus(cid)).isEqualTo("EXHAUSTED"));
        assertThat(jdbc.sql("SELECT strategy || '/' || status FROM quarantine_event WHERE correlation_id = :c")
                .param("c", cid).query(String.class).single()).isEqualTo("TRANSIENT/PENDING_REVIEW");
        assertThat(auditRows(cid, "RETRY_SCHEDULED")).isEqualTo(3);
        assertThat(auditRows(cid, "RETRY_REPLAYED")).isEqualTo(3);
        assertThat(auditRows(cid, "QUARANTINED")).isEqualTo(1);

        // Nothing more is replayed once the budget is spent: exactly 3 copies reached the topic.
        var all = MainTopicReader.readAll(KAFKA.getBootstrapServers(), key, Duration.ofSeconds(4));
        assertThat(all).hasSize(3);
        assertThat(all).extracting(x -> MainTopicReader.header(x, "x-retry-attempt")).containsExactly("1", "2", "3");
        assertThat(all).extracting(x -> x.value()).containsOnly(original);
    }

    @Test
    void severalPollersNeverReplayTheSameEventTwice() throws Exception {
        String key = "o-" + UUID.randomUUID();
        String cid = "cid-" + UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO retry_schedule (id, fingerprint, correlation_id, target_topic, kafka_key, payload,
                                            attempt, next_attempt_at, status, last_event_id, last_error)
                VALUES (:id, :f, :c, 'orders-topic', :k, :p, 0, now() - interval '1 minute', 'SCHEDULED', :e, 'seeded')
                """)
                .param("id", UUID.randomUUID()).param("f", "%064x".formatted(Math.abs(UUID.randomUUID().getMostSignificantBits())))
                .param("c", cid).param("k", key).param("p", payload(key)).param("e", UUID.randomUUID()).update();

        int threads = 6;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<java.util.concurrent.Future<Integer>> sent = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            sent.add(pool.submit(() -> {
                go.await();
                return poller.pollOnce();
            }));
        }
        go.countDown();
        for (var f : sent) {
            f.get();
        }
        pool.shutdown();

        // Whoever got there first replayed it once; the scheduled poller is also running and must not add a second.
        assertThat(MainTopicReader.readAll(KAFKA.getBootstrapServers(), key, Duration.ofSeconds(5))).hasSize(1);
        assertThat(retryStatus(cid)).isEqualTo("REPLAYED");
    }

    @Test
    void anOpenBreakerHoldsReplaysBackThenLetsOneCanaryThrough() throws Exception {
        String key = "o-" + UUID.randomUUID();
        String cid = "cid-" + UUID.randomUUID();
        for (int i = 0; i < 4; i++) { // breaker.failure-threshold = 4 in tests
            breaker.recordRetryFailure();
        }
        assertThat(breaker.state()).isEqualTo(ReplayCircuitBreaker.State.OPEN);
        jdbc.sql("""
                INSERT INTO retry_schedule (id, fingerprint, correlation_id, target_topic, kafka_key, payload,
                                            attempt, next_attempt_at, status, last_event_id, last_error)
                VALUES (:id, :f, :c, 'orders-topic', :k, :p, 0, now() - interval '1 minute', 'SCHEDULED', :e, 'seeded')
                """)
                .param("id", UUID.randomUUID()).param("f", "%064x".formatted(Math.abs(UUID.randomUUID().getMostSignificantBits())))
                .param("c", cid).param("k", key).param("p", payload(key)).param("e", UUID.randomUUID()).update();

        // The row is due and the poller runs every 200 ms, but the breaker (3 s cooldown) holds it back.
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(retryStatus(cid)).isEqualTo("SCHEDULED"));

        // After the cooldown the canary goes out.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(retryStatus(cid)).isEqualTo("REPLAYED"));
        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), key, Duration.ofSeconds(10))).hasSize(1);

        // Nothing came back for the canary, so the breaker closes again.
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(breaker.state()).isEqualTo(ReplayCircuitBreaker.State.CLOSED));
    }
}
