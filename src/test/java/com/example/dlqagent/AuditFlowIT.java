package com.example.dlqagent;

import static com.example.dlqagent.support.ScriptedChatModel.decision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.dlqagent.agent.DlqTriageService;
import com.example.dlqagent.domain.AuditEntry;
import com.example.dlqagent.domain.AuditLog;
import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.domain.FieldChange;
import com.example.dlqagent.domain.QuarantineRepository;
import com.example.dlqagent.domain.RemediationPlan;
import com.example.dlqagent.domain.RemediationStrategy;
import com.example.dlqagent.domain.RepairResult;
import com.example.dlqagent.infrastructure.DlqAgentProperties;
import com.example.dlqagent.observability.TraceContext;
import com.example.dlqagent.retry.RetryScheduler;
import com.example.dlqagent.support.MainTopicReader;
import com.example.dlqagent.support.ScriptedChatModel;
import com.example.dlqagent.support.TestStreams;
import io.micrometer.observation.ObservationRegistry;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.transaction.support.TransactionTemplate;

/** The audit trail and the traces, end to end: real Kafka, Postgres, Spring Security and OpenTelemetry spans. */
class AuditFlowIT extends AbstractIntegrationTest {

    private static final String DEFAULT_REPLY = decision("POISON_PILL", 0.95, "stubbed poison pill");
    private static final RequestPostProcessor REVIEWER = httpBasic("reviewer", "test-reviewer-password");

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired JdbcClient jdbc;
    @Autowired ScriptedChatModel llm;
    @Autowired MockMvc mvc;
    @Autowired AuditLog auditLog;
    @Autowired QuarantineRepository quarantine;
    @Autowired TransactionTemplate tx;
    @Autowired DlqAgentProperties props;
    @Autowired TraceContext traceContext;
    @Autowired RetryScheduler retryScheduler;
    @Autowired InMemorySpanExporter spans;
    @Autowired SdkTracerProvider tracerProvider;

    @AfterEach
    void resetLlm() {
        llm.respondWith(p -> DEFAULT_REPLY);
    }

    private static String order(String orderId, String amount, String createdAt) {
        return """
                {"orderId":"%s","customerId":"c-9","amount":%s,"currency":"USD","status":"PAID","createdAt":"%s"}"""
                .formatted(orderId, amount, createdAt);
    }

    private void scriptRepair(String edits) {
        llm.respondWith(p -> ScriptedChatModel.isPlanner(p)
                ? decision("SCHEMA_DRIFT", 0.95, "format drift in the payload")
                : "{\"repairable\":true,\"confidence\":0.93,\"summary\":\"fixed the format\",\"edits\":" + edits
                        + ",\"dropFields\":[]}");
    }

    private String send(String key, String payload) throws Exception {
        String cid = "cid-" + UUID.randomUUID();
        var record = new ProducerRecord<>("orders-dlq", key, payload);
        record.headers().add("x-correlation-id", cid.getBytes(StandardCharsets.UTF_8));
        record.headers().add("kafka_dlt-original-topic", "orders-topic".getBytes(StandardCharsets.UTF_8));
        kafka.send(record).get();
        return cid;
    }

    private List<AuditEntry> awaitAudit(String cid, int expected) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(auditLog.findByCorrelationId(cid, 10)).hasSize(expected));
        return auditLog.findByCorrelationId(cid, 10);
    }

    // ---- what each outcome records ------------------------------------------------------------------------

    @Test
    void tier1ReplayIsAuditedWithReasoningDiffAndTraceId() throws Exception {
        scriptRepair("[{\"field\":\"amount\",\"newValue\":\"19.99\"}]");
        String key = "o-" + UUID.randomUUID();

        String cid = send(key, order(key, "\"19.99 USD\"", "2025-04-03T10:00:00Z"));

        AuditEntry a = awaitAudit(cid, 1).get(0);
        assertThat(a.stage()).isEqualTo(AuditEntry.Stage.TRIAGE);
        assertThat(a.tier()).isEqualTo(AuditEntry.Tier.TIER_1);
        assertThat(a.outcome()).isEqualTo(AuditEntry.Outcome.REPLAYED);
        assertThat(a.actor()).isEqualTo("agent");
        assertThat(a.strategy()).isEqualTo(RemediationStrategy.SCHEMA_DRIFT);
        assertThat(a.plannerRationale()).isEqualTo("format drift in the payload");
        assertThat(a.plannerConfidence()).isEqualTo(0.95);
        assertThat(a.repairSummary()).isEqualTo("fixed the format");
        assertThat(a.repairConfidence()).isEqualTo(0.93);
        assertThat(a.changes()).containsExactly(new FieldChange("amount", "\"19.99 USD\"", "19.99"));
        assertThat(a.riskFlags()).isEmpty();
        assertThat(a.originalTopic()).isEqualTo("orders-topic");
        assertThat(a.key()).isEqualTo(key);
        assertThat(a.durationMs()).isNotNegative();
        assertThat(a.traceId()).matches("[0-9a-f]{32}");
        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), key, Duration.ofSeconds(30))).hasSize(1);
    }

    @Test
    void poisonPillIsAuditedWithThePlannersRationale() throws Exception {
        String key = "o-" + UUID.randomUUID();

        String cid = send(key, "{\"orderId\": corrupt");

        AuditEntry a = awaitAudit(cid, 1).get(0);
        assertThat(a.outcome()).isEqualTo(AuditEntry.Outcome.QUARANTINED);
        assertThat(a.tier()).isEqualTo(AuditEntry.Tier.NONE);
        assertThat(a.strategy()).isEqualTo(RemediationStrategy.POISON_PILL);
        assertThat(a.plannerRationale()).isEqualTo("stubbed poison pill");
        assertThat(a.outcomeReason()).isEqualTo("stubbed poison pill");
        assertThat(a.changes()).isEmpty();
        assertThat(a.traceId()).matches("[0-9a-f]{32}");
    }

    @Test
    void humanApprovalAddsASecondRowForTheSameEventAndTheAuditApiShowsBoth() throws Exception {
        scriptRepair("[{\"field\":\"createdAt\",\"newValue\":\"2025-04-03T00:00:00Z\"}]");
        String key = "o-" + UUID.randomUUID();
        String cid = send(key, order(key, "19.99", "03/04/2025")); // ambiguous date: Tier 2

        AuditEntry held = awaitAudit(cid, 1).get(0);
        assertThat(held.tier()).isEqualTo(AuditEntry.Tier.TIER_2);
        assertThat(held.outcome()).isEqualTo(AuditEntry.Outcome.HELD_FOR_APPROVAL);
        assertThat(held.riskFlags()).anyMatch(f -> f.contains("ambiguous"));
        assertThat(held.changes()).hasSize(1);

        String id = held.eventId().toString();
        mvc.perform(post("/api/approvals/" + id + "/approve").with(REVIEWER)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"notes\":\"confirmed\"}"))
                .andExpect(status().isOk());

        List<AuditEntry> trail = awaitAudit(cid, 2);
        AuditEntry decision = trail.get(1);
        assertThat(decision.stage()).isEqualTo(AuditEntry.Stage.APPROVAL);
        assertThat(decision.outcome()).isEqualTo(AuditEntry.Outcome.APPROVED_REPLAYED);
        assertThat(decision.actor()).isEqualTo("reviewer");
        assertThat(decision.eventId()).isEqualTo(held.eventId());
        assertThat(decision.outcomeReason()).isEqualTo("Approved by reviewer: confirmed");
        assertThat(decision.riskFlags()).isEqualTo(held.riskFlags());
        assertThat(decision.traceId()).matches("[0-9a-f]{32}").isNotEqualTo(held.traceId()); // a separate request

        mvc.perform(get("/api/audit").param("correlationId", cid).with(REVIEWER))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].outcome").value("HELD_FOR_APPROVAL"))
                .andExpect(jsonPath("$[1].outcome").value("APPROVED_REPLAYED"))
                .andExpect(jsonPath("$[1].actor").value("reviewer"));
    }

    @Test
    void rejectionIsAudited() throws Exception {
        scriptRepair("[{\"field\":\"createdAt\",\"newValue\":\"2025-04-03T00:00:00Z\"}]");
        String key = "o-" + UUID.randomUUID();
        String cid = send(key, order(key, "19.99", "03/04/2025"));
        AuditEntry held = awaitAudit(cid, 1).get(0);

        mvc.perform(post("/api/approvals/" + held.eventId() + "/reject").with(REVIEWER)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"notes\":\"wrong order\"}"))
                .andExpect(status().isOk());

        AuditEntry decision = awaitAudit(cid, 2).get(1);
        assertThat(decision.outcome()).isEqualTo(AuditEntry.Outcome.REJECTED);
        assertThat(decision.outcomeReason()).isEqualTo("Rejected by reviewer: wrong order");
    }

    @Test
    void theAuditApiRequiresCredentials() throws Exception {
        mvc.perform(get("/api/audit").param("correlationId", "x")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/audit").with(REVIEWER)).andExpect(status().isBadRequest()); // correlationId is required
        mvc.perform(get("/api/audit").param("correlationId", "no-such-id").with(REVIEWER))
                .andExpect(status().isOk()).andExpect(jsonPath("$.length()").value(0));
    }

    // ---- integrity ------------------------------------------------------------------------------------------

    @Test
    void theAuditLogIsAppendOnly() throws Exception {
        String key = "o-" + UUID.randomUUID();
        String cid = send(key, "garbage");
        awaitAudit(cid, 1);

        assertThatThrownBy(() -> jdbc.sql("UPDATE dlq_audit_log SET outcome_reason = 'edited' WHERE correlation_id = :c")
                .param("c", cid).update()).hasMessageContaining("append-only");
        assertThatThrownBy(() -> jdbc.sql("DELETE FROM dlq_audit_log WHERE correlation_id = :c")
                .param("c", cid).update()).hasMessageContaining("append-only");
        assertThat(auditLog.findByCorrelationId(cid, 10)).hasSize(1);
    }

    @Test
    void hostilePayloadContentWithNulCharactersCannotBreakTheAuditWrite() {
        UUID eventId = UUID.randomUUID();
        var entry = new AuditEntry(AuditEntry.idFor(eventId, AuditEntry.Stage.TRIAGE), eventId, "orders", "cid-nul-" + eventId,
                null, Instant.now(), AuditEntry.Stage.TRIAGE, "orders-topic", "k\u0000", RemediationStrategy.SCHEMA_DRIFT,
                0.5, "rationale\u0000text", "summary", 0.5, List.of(),
                List.of(new FieldChange("f\u0000", "before\u0000", "after")), List.of("flag\u0000"),
                AuditEntry.Tier.TIER_2, AuditEntry.Outcome.HELD_FOR_APPROVAL, "reason", "agent", 5);

        assertThat(auditLog.record(entry)).isTrue();

        AuditEntry stored = auditLog.findByCorrelationId("cid-nul-" + eventId, 1).get(0);
        assertThat(stored.plannerRationale()).isEqualTo("rationale�text");
        assertThat(stored.changes()).containsExactly(new FieldChange("f�", "before�", "after"));
        assertThat(stored.riskFlags()).containsExactly("flag�");
        assertThat(stored.key()).isEqualTo("k�");
    }

    @Test
    void recordingTheSameEntryTwiceWritesOneRow() {
        UUID eventId = UUID.randomUUID();
        var entry = new AuditEntry(AuditEntry.idFor(eventId, AuditEntry.Stage.TRIAGE), eventId, "orders", "cid-dup-" + eventId,
                null, Instant.now(), AuditEntry.Stage.TRIAGE, "orders-topic", null, RemediationStrategy.POISON_PILL,
                null, null, null, null, List.of(), List.of(), List.of(), AuditEntry.Tier.NONE,
                AuditEntry.Outcome.QUARANTINED, "r", "agent", 1);

        assertThat(auditLog.record(entry)).isTrue();
        assertThat(auditLog.record(entry)).isFalse();
    }

    @Test
    void ifTheAuditWriteFailsTheQuarantineRowRollsBackWithIt() {
        AuditLog failing = new AuditLog() {
            @Override
            public boolean record(AuditEntry entry) {
                throw new IllegalStateException("audit store down");
            }

            @Override
            public List<AuditEntry> findByCorrelationId(String correlationId, int limit) {
                return List.of();
            }
        };
        var service = new DlqTriageService(
                c -> new RemediationPlan.Quarantine("poison"), c -> new RepairResult.Unrepairable("unused"),
                request -> { }, quarantine, failing, ObservationRegistry.NOOP, tx, traceContext,
                retryScheduler);
        var ctx = new FailureContext("orders", "cid-atomic-" + UUID.randomUUID(), "orders-dlq", 0, System.nanoTime() & 0xFFFFFFF,
                "orders-topic", "k", "garbage", Map.of(), null, Instant.now());

        assertThatThrownBy(() -> service.triage(TestStreams.orders(), ctx)).hasMessage("audit store down");

        // A held event without an audit record must be impossible; the listener will retry the whole thing.
        assertThat(quarantine.findById(ctx.eventId())).isEmpty();
    }

    // ---- traces ---------------------------------------------------------------------------------------------

    @Test
    void oneTraceLinksTheKafkaConsumeThePlanTheRepairTheReplayAndTheAuditRow() throws Exception {
        scriptRepair("[{\"field\":\"amount\",\"newValue\":\"19.99\"}]");
        String key = "o-" + UUID.randomUUID();
        String cid = send(key, order(key, "\"19.99 USD\"", "2025-04-03T10:00:00Z"));
        AuditEntry a = awaitAudit(cid, 1).get(0);
        MainTopicReader.read(KAFKA.getBootstrapServers(), key, Duration.ofSeconds(30));

        tracerProvider.forceFlush().join(10, TimeUnit.SECONDS);
        List<SpanData> trace = spans.getFinishedSpanItems().stream()
                .filter(s -> s.getTraceId().equals(a.traceId())).toList();

        assertThat(trace).extracting(SpanData::getName)
                .contains("dlq.triage", "dlq.plan", "dlq.repair", "dlq.replay")
                .contains("orders-dlq receive")   // Spring Kafka: the listener consuming the DLQ record
                .contains("orders-topic send");   // Spring Kafka: the replay publish
        // Spring AI's own observations: one ChatClient call for the planner and one for the repair agent.
        assertThat(trace.stream().filter(s -> s.getName().equals("spring_ai chat_client"))).hasSize(2);
        SpanData triage = trace.stream().filter(s -> s.getName().equals("dlq.triage")).findFirst().orElseThrow();
        for (String child : List.of("dlq.plan", "dlq.repair", "dlq.replay")) {
            SpanData s = trace.stream().filter(x -> x.getName().equals(child)).findFirst().orElseThrow();
            assertThat(s.getParentSpanId()).as(child + " is a child of dlq.triage").isEqualTo(triage.getSpanId());
        }
        var attrs = triage.getAttributes().asMap();
        assertThat(attrs.toString()).contains("correlation.id", cid).contains("outcome", "REPLAYED").contains("tier", "TIER_1");
    }
}
