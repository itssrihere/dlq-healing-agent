package com.example.dlqagent;

import static com.example.dlqagent.support.ScriptedChatModel.decision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.dlqagent.retry.ReplayCircuitBreaker;
import com.example.dlqagent.stream.StreamRegistry;
import com.example.dlqagent.support.MainTopicReader;
import com.example.dlqagent.support.ScriptedChatModel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

/**
 * Two streams (orders and shipments, see application-test.yml) healed by one service, against real Kafka and Postgres
 * with a scripted model. Proves the properties that make several DLQs safe to run together: each stream is judged by
 * its own contract, replays only to its own source topic, cannot be tricked by a header naming another stream's topic,
 * keeps its own circuit breaker, and can be reviewed separately.
 */
class MultiStreamIT extends AbstractIntegrationTest {

    private static final String DEFAULT_REPLY = decision("POISON_PILL", 0.95, "stubbed poison pill");
    private static final RequestPostProcessor REVIEWER = httpBasic("reviewer", "test-reviewer-password");

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired JdbcClient jdbc;
    @Autowired ScriptedChatModel llm;
    @Autowired MockMvc mvc;
    @Autowired StreamRegistry streams;

    @AfterEach
    void resetLlm() {
        llm.respondWith(p -> DEFAULT_REPLY);
    }

    // ---- helpers ---------------------------------------------------------------------------------------------

    private static String order(String id, String amount) {
        return "{\"orderId\":\"%s\",\"customerId\":\"c-9\",\"amount\":%s,\"currency\":\"USD\",\"status\":\"PAID\",\"createdAt\":\"2025-04-03T10:00:00Z\"}"
                .formatted(id, amount);
    }

    private static String shipment(String id, String quantity, String shippedOn) {
        return "{\"shipmentId\":\"%s\",\"orderRef\":\"o-1\",\"carrier\":\"DHL\",\"quantity\":%s,\"shippedOn\":\"%s\"}"
                .formatted(id, quantity, shippedOn);
    }

    private static String shipmentId() {
        return "S-%06d".formatted(ThreadLocalRandom.current().nextInt(1_000_000));
    }

    /** The scripted model answers per stream: the system prompt names the topic whose contract it is shown. */
    private void scriptRepairs(String shipmentEdit, String orderEdit) {
        llm.respondWith(p -> {
            if (ScriptedChatModel.isPlanner(p)) {
                return decision("SCHEMA_DRIFT", 0.95, "a value is in the wrong format");
            }
            String edit = ScriptedChatModel.systemText(p).contains("TOPIC CONTRACT for shipments-topic")
                    ? shipmentEdit : orderEdit;
            return "{\"repairable\":true,\"confidence\":0.95,\"summary\":\"fix format\",\"edits\":" + edit
                    + ",\"dropFields\":[]}";
        });
    }

    private void send(String dlqTopic, String key, String payload, String originalTopic, String cid) throws Exception {
        var record = new ProducerRecord<>(dlqTopic, key, payload);
        record.headers().add("x-correlation-id", cid.getBytes(StandardCharsets.UTF_8));
        record.headers().add("kafka_dlt-original-topic", originalTopic.getBytes(StandardCharsets.UTF_8));
        kafka.send(record).get();
    }

    private String streamOfAudit(String cid) {
        return jdbc.sql("SELECT DISTINCT stream FROM dlq_audit_log WHERE correlation_id = :c")
                .param("c", cid).query(String.class).single();
    }

    private int count(String sql, String cid) {
        return jdbc.sql(sql).param("c", cid).query(Integer.class).single();
    }

    // ---- tests -----------------------------------------------------------------------------------------------

    @Test
    void eachStreamIsRepairedByItsOwnContractAndReplayedToItsOwnTopic() throws Exception {
        scriptRepairs("[{\"field\":\"quantity\",\"newValue\":\"3\"}]", "[{\"field\":\"amount\",\"newValue\":\"19.99\"}]");
        String shipId = shipmentId();
        String orderId = "o-" + UUID.randomUUID();
        String shipCid = "cid-" + UUID.randomUUID();
        String orderCid = "cid-" + UUID.randomUUID();

        send("shipments-dlq", shipId, shipment(shipId, "\"3 units\"", "2025-04-03"), "shipments-topic", shipCid);
        send("orders-dlq", orderId, order(orderId, "\"19.99 USD\""), "orders-topic", orderCid);

        var shipped = MainTopicReader.read(KAFKA.getBootstrapServers(), "shipments-topic", shipId, Duration.ofSeconds(30));
        var ordered = MainTopicReader.read(KAFKA.getBootstrapServers(), "orders-topic", orderId, Duration.ofSeconds(30));
        assertThat(shipped).hasSize(1);
        assertThat(shipped.get(0).value()).contains("\"quantity\":3").doesNotContain("units");
        assertThat(MainTopicReader.header(shipped.get(0), "x-repaired-by-agent")).isEqualTo("true");
        assertThat(ordered).hasSize(1);
        assertThat(ordered.get(0).value()).contains("\"amount\":19.99");

        // nothing crossed over
        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), "orders-topic", shipId, Duration.ofSeconds(2))).isEmpty();
        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), "shipments-topic", orderId, Duration.ofSeconds(2))).isEmpty();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(streamOfAudit(shipCid)).isEqualTo("shipments");
            assertThat(streamOfAudit(orderCid)).isEqualTo("orders");
        });
    }

    @Test
    void aHeaderNamingAnotherStreamsTopicIsQuarantinedNotReplayedThere() throws Exception {
        scriptRepairs("[{\"field\":\"quantity\",\"newValue\":\"3\"}]", "[{\"field\":\"amount\",\"newValue\":\"19.99\"}]");
        String shipId = shipmentId();
        String cid = "cid-" + UUID.randomUUID();

        // a shipments DLQ record claiming it came from orders-topic: a producer's claim, not a fact
        send("shipments-dlq", shipId, shipment(shipId, "\"3 units\"", "2025-04-03"), "orders-topic", cid);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c", cid)).isEqualTo(1));
        var row = jdbc.sql("SELECT stream, status, reason FROM quarantine_event WHERE correlation_id = :c")
                .param("c", cid).query().singleRow();
        assertThat(row.get("stream")).isEqualTo("shipments");
        assertThat(row.get("status")).isEqualTo("PENDING_REVIEW");
        assertThat(row.get("reason").toString()).contains("unexpected topic 'orders-topic'");
        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), "orders-topic", shipId, Duration.ofSeconds(3))).isEmpty();
        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), "shipments-topic", shipId, Duration.ofSeconds(3))).isEmpty();
    }

    @Test
    void aMissingOriginalTopicHeaderDefaultsToTheStreamsOwnSourceTopic() throws Exception {
        scriptRepairs("[{\"field\":\"quantity\",\"newValue\":\"3\"}]", "[{\"field\":\"amount\",\"newValue\":\"19.99\"}]");
        String shipId = shipmentId();
        var record = new ProducerRecord<>("shipments-dlq", shipId, shipment(shipId, "\"3 units\"", "2025-04-03"));
        kafka.send(record).get(); // no headers at all

        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), "shipments-topic", shipId, Duration.ofSeconds(30)))
                .hasSize(1);
    }

    @Test
    void oneStreamsOpenBreakerDoesNotHoldBackAnotherStreamsRetries() {
        ReplayCircuitBreaker shipmentsBreaker = streams.byName("shipments").orElseThrow().breaker();
        for (int i = 0; i < 4; i++) { // breaker.failure-threshold = 4 in tests
            shipmentsBreaker.recordRetryFailure();
        }
        assertThat(shipmentsBreaker.state()).isEqualTo(ReplayCircuitBreaker.State.OPEN);
        String shipId = shipmentId();
        String orderId = "o-" + UUID.randomUUID();
        String shipCid = "cid-" + UUID.randomUUID();
        String orderCid = "cid-" + UUID.randomUUID();
        seedRetry("shipments", "shipments-topic", shipId, shipment(shipId, "3", "2025-04-03"), shipCid);
        seedRetry("orders", "orders-topic", orderId, order(orderId, "19.99"), orderCid);

        // both rows are due and polled every 200 ms; the open breaker (3 s cooldown) holds back only shipments
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(retryStatus(orderCid)).isEqualTo("REPLAYED"));
        assertThat(retryStatus(shipCid)).isEqualTo("SCHEDULED");

        // then the shipments canary goes out and its breaker closes again
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(retryStatus(shipCid)).isEqualTo("REPLAYED"));
        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), "shipments-topic", shipId, Duration.ofSeconds(10))).hasSize(1);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(shipmentsBreaker.state()).isEqualTo(ReplayCircuitBreaker.State.CLOSED));
    }

    private void seedRetry(String stream, String topic, String key, String payload, String cid) {
        jdbc.sql("""
                INSERT INTO retry_schedule (id, stream, fingerprint, correlation_id, target_topic, kafka_key, payload,
                                            attempt, next_attempt_at, status, last_event_id, last_error)
                VALUES (:id, :stream, :f, :c, :topic, :k, :p, 0, now() - interval '1 minute', 'SCHEDULED', :e, 'seeded')
                """)
                .param("id", UUID.randomUUID()).param("stream", stream)
                .param("f", "%064x".formatted(Math.abs(UUID.randomUUID().getMostSignificantBits())))
                .param("c", cid).param("topic", topic).param("k", key).param("p", payload)
                .param("e", UUID.randomUUID()).update();
    }

    private String retryStatus(String cid) {
        return jdbc.sql("SELECT status FROM retry_schedule WHERE correlation_id = :c")
                .param("c", cid).query(String.class).optional().orElse("(none)");
    }

    @Test
    void approvalsCanBeListedPerStreamAndAnApprovedFixGoesToItsOwnStreamsTopic() throws Exception {
        // an ambiguous date is a guess in both contracts, so both streams hold their repair for a human
        scriptRepairs("[{\"field\":\"shippedOn\",\"newValue\":\"2025-04-03\"}]",
                "[{\"field\":\"createdAt\",\"newValue\":\"2025-04-03T00:00:00Z\"}]");
        String shipId = shipmentId();
        String orderId = "o-" + UUID.randomUUID();
        String shipCid = "cid-" + UUID.randomUUID();
        String orderCid = "cid-" + UUID.randomUUID();
        send("shipments-dlq", shipId, shipment(shipId, "3", "03/04/2025"), "shipments-topic", shipCid);
        send("orders-dlq", orderId, order(orderId, "19.99").replace("2025-04-03T10:00:00Z", "03/04/2025"),
                "orders-topic", orderCid);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(count("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c", shipCid)).isEqualTo(1);
            assertThat(count("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c", orderCid)).isEqualTo(1);
        });
        String shipItem = jdbc.sql("SELECT id::text FROM quarantine_event WHERE correlation_id = :c")
                .param("c", shipCid).query(String.class).single();
        String orderItem = jdbc.sql("SELECT id::text FROM quarantine_event WHERE correlation_id = :c")
                .param("c", orderCid).query(String.class).single();

        mvc.perform(get("/api/approvals").param("stream", "shipments").with(REVIEWER))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.id=='" + shipItem + "')]").isNotEmpty())
                .andExpect(jsonPath("$[?(@.id=='" + orderItem + "')]").isEmpty())
                .andExpect(jsonPath("$[?(@.stream!='shipments')]").isEmpty());
        mvc.perform(get("/api/approvals").param("stream", "orders").with(REVIEWER))
                .andExpect(jsonPath("$[?(@.id=='" + orderItem + "')]").isNotEmpty())
                .andExpect(jsonPath("$[?(@.id=='" + shipItem + "')]").isEmpty());
        mvc.perform(get("/api/approvals").with(REVIEWER)) // no filter: both
                .andExpect(jsonPath("$[?(@.id=='" + shipItem + "')]").isNotEmpty())
                .andExpect(jsonPath("$[?(@.id=='" + orderItem + "')]").isNotEmpty());

        mvc.perform(post("/api/approvals/" + shipItem + "/approve").with(REVIEWER)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"notes\":\"3 April\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.stream").value("shipments"));

        var replayed = MainTopicReader.read(KAFKA.getBootstrapServers(), "shipments-topic", shipId, Duration.ofSeconds(30));
        assertThat(replayed).hasSize(1);
        assertThat(replayed.get(0).value()).contains("\"shippedOn\":\"2025-04-03\"");
        assertThat(MainTopicReader.header(replayed.get(0), "x-approved-by")).isEqualTo("reviewer");
        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), "orders-topic", shipId, Duration.ofSeconds(2))).isEmpty();
    }
}
