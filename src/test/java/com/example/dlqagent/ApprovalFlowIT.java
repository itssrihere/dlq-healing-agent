package com.example.dlqagent;

import static com.example.dlqagent.support.ScriptedChatModel.decision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.dlqagent.support.MainTopicReader;
import com.example.dlqagent.support.ScriptedChatModel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
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
 * Tier 2 end to end: an assumption-laden repair is held in Postgres (not replayed), and a human decision made
 * through the secured API either replays it with attribution or discards it. Real Kafka, Postgres, Spring Security.
 */
class ApprovalFlowIT extends AbstractIntegrationTest {

    private static final String DEFAULT_REPLY = decision("POISON_PILL", 0.95, "stubbed poison pill");
    private static final RequestPostProcessor REVIEWER = httpBasic("reviewer", "test-reviewer-password");

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired JdbcClient jdbc;
    @Autowired ScriptedChatModel llm;
    @Autowired MockMvc mvc;

    @AfterEach
    void resetLlm() {
        llm.respondWith(p -> DEFAULT_REPLY);
    }

    /** An ambiguous date (03/04/2025): the model's reformat is valid, but day/month order is a guess. */
    private static String ambiguous(String orderId) {
        return """
                {"orderId":"%s","customerId":"c-9","amount":19.99,"currency":"USD","status":"PAID","createdAt":"03/04/2025"}"""
                .formatted(orderId);
    }

    /** Seeds one Tier 2 row through the real pipeline and returns {id, orderId, correlationId}. */
    private String[] seedTier2() throws Exception {
        llm.respondWith(p -> ScriptedChatModel.isPlanner(p)
                ? decision("SCHEMA_DRIFT", 0.95, "date in a non ISO format")
                : "{\"repairable\":true,\"confidence\":0.95,\"summary\":\"assumed day first\",\"edits\":"
                        + "[{\"field\":\"createdAt\",\"newValue\":\"2025-04-03T00:00:00Z\"}],\"dropFields\":[]}");
        String key = "o-" + UUID.randomUUID();
        String cid = "cid-" + UUID.randomUUID();
        var record = new ProducerRecord<>("orders-dlq", key, ambiguous(key));
        record.headers().add("x-correlation-id", cid.getBytes(StandardCharsets.UTF_8));
        record.headers().add("kafka_dlt-original-topic", "orders-topic".getBytes(StandardCharsets.UTF_8));
        record.headers().add("tenant", "acme".getBytes(StandardCharsets.UTF_8));
        kafka.send(record).get();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(jdbc.sql("SELECT count(*) FROM quarantine_event WHERE correlation_id = :c")
                        .param("c", cid).query(Integer.class).single()).isEqualTo(1));
        String id = jdbc.sql("SELECT id::text FROM quarantine_event WHERE correlation_id = :c")
                .param("c", cid).query(String.class).single();
        return new String[] {id, key, cid};
    }

    private String statusOf(String id) {
        return jdbc.sql("SELECT status FROM quarantine_event WHERE id = CAST(:id AS uuid)")
                .param("id", id).query(String.class).single();
    }

    // ---- the Tier 2 row itself ------------------------------------------------------------------------------

    @Test
    void assumptionLadenRepairIsHeldForApprovalAndNotReplayed() throws Exception {
        String[] seed = seedTier2();

        var row = jdbc.sql("SELECT * FROM quarantine_event WHERE id = CAST(:id AS uuid)")
                .param("id", seed[0]).query().singleRow();
        assertThat(row.get("status")).isEqualTo("PENDING_HUMAN_APPROVAL");
        assertThat(row.get("strategy")).isEqualTo("SCHEMA_DRIFT");
        assertThat(row.get("payload").toString()).contains("03/04/2025"); // original preserved
        assertThat(row.get("proposed_payload").toString()).contains("2025-04-03T00:00:00Z");
        assertThat(row.get("repair_summary")).isEqualTo("assumed day first");
        assertThat(row.get("risk_flags").toString()).contains("ambiguous", "time of day and zone were assumed");
        assertThat(row.get("reason").toString()).startsWith("Awaiting human approval");
        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), seed[1], Duration.ofSeconds(3))).isEmpty();
    }

    // ---- security -------------------------------------------------------------------------------------------

    @Test
    void theApiRequiresCredentialsAndRejectsWrongOnes() throws Exception {
        mvc.perform(get("/api/approvals")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/approvals").with(httpBasic("reviewer", "wrong"))).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/approvals").with(httpBasic("mallory", "test-reviewer-password")))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/api/approvals/" + UUID.randomUUID() + "/approve")).andExpect(status().isUnauthorized());
    }

    @Test
    void everythingExceptHealthAndTheApprovalApiIsDenied() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk());
        mvc.perform(get("/actuator/info").with(REVIEWER)).andExpect(status().isForbidden());
        mvc.perform(get("/anything-else").with(REVIEWER)).andExpect(status().isForbidden());
    }

    // ---- reviewing ------------------------------------------------------------------------------------------

    @Test
    void reviewerSeesTheOriginalTheProposalAndAFieldLevelDiff() throws Exception {
        String[] seed = seedTier2();

        mvc.perform(get("/api/approvals/" + seed[0]).with(REVIEWER))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING_HUMAN_APPROVAL"))
                .andExpect(jsonPath("$.correlationId").value(seed[2]))
                .andExpect(jsonPath("$.repairSummary").value("assumed day first"))
                .andExpect(jsonPath("$.riskFlags[0]").exists())
                .andExpect(jsonPath("$.changes.length()").value(1))
                .andExpect(jsonPath("$.changes[0].field").value("createdAt"))
                .andExpect(jsonPath("$.changes[0].before").value("\"03/04/2025\""))
                .andExpect(jsonPath("$.changes[0].after").value("\"2025-04-03T00:00:00Z\""));

        mvc.perform(get("/api/approvals").with(REVIEWER)).andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.id == '" + seed[0] + "')]").exists());
    }

    @Test
    void approvingReplaysWithAttributionAndCannotBeDoneTwice() throws Exception {
        String[] seed = seedTier2();

        mvc.perform(post("/api/approvals/" + seed[0] + "/approve").with(REVIEWER)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"notes\":\"checked with the customer\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("APPROVED_REPLAYED"))
                .andExpect(jsonPath("$.reviewedBy").value("reviewer"))
                .andExpect(jsonPath("$.reviewNotes").value("checked with the customer"));

        var replayed = MainTopicReader.read(KAFKA.getBootstrapServers(), seed[1], Duration.ofSeconds(30));
        assertThat(replayed).hasSize(1);
        var r = replayed.get(0);
        assertThat(r.value()).contains("2025-04-03T00:00:00Z").doesNotContain("03/04/2025");
        assertThat(MainTopicReader.header(r, "x-repaired-by-agent")).isEqualTo("true");
        assertThat(MainTopicReader.header(r, "x-approved-by")).isEqualTo("reviewer");
        assertThat(MainTopicReader.header(r, "x-correlation-id")).isEqualTo(seed[2]);
        assertThat(MainTopicReader.header(r, "x-repair-summary")).isEqualTo("assumed day first");
        assertThat(MainTopicReader.header(r, "tenant")).isEqualTo("acme");

        mvc.perform(post("/api/approvals/" + seed[0] + "/approve").with(REVIEWER).contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isConflict());
        mvc.perform(post("/api/approvals/" + seed[0] + "/reject").with(REVIEWER)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"notes\":\"too late\"}"))
                .andExpect(status().isConflict());
    }

    @Test
    void rejectingNeedsANoteAndNeverReplays() throws Exception {
        String[] seed = seedTier2();

        mvc.perform(post("/api/approvals/" + seed[0] + "/reject").with(REVIEWER)
                        .contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/api/approvals/" + seed[0] + "/reject").with(REVIEWER)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"notes\":\"   \"}"))
                .andExpect(status().isBadRequest());
        assertThat(statusOf(seed[0])).isEqualTo("PENDING_HUMAN_APPROVAL");

        mvc.perform(post("/api/approvals/" + seed[0] + "/reject").with(REVIEWER)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"notes\":\"day and month are the other way\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("REJECTED"))
                .andExpect(jsonPath("$.reviewedBy").value("reviewer"));

        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), seed[1], Duration.ofSeconds(3))).isEmpty();
        mvc.perform(post("/api/approvals/" + seed[0] + "/approve").with(REVIEWER).contentType(MediaType.APPLICATION_JSON).content("{}")).andExpect(status().isConflict());
    }

    @Test
    void approvalIsRefusedIfTheStoredProposalNoLongerPassesTheHardChecks() throws Exception {
        String[] seed = seedTier2();
        // Someone tampers with the row (or the rules tightened): change the amount, which is a hard violation.
        jdbc.sql("UPDATE quarantine_event SET proposed_payload = replace(proposed_payload, '19.99', '99.99') "
                + "WHERE id = CAST(:id AS uuid)").param("id", seed[0]).update();

        mvc.perform(post("/api/approvals/" + seed[0] + "/approve").with(REVIEWER).contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value(org.hamcrest.Matchers.containsString("amount value changed")));

        assertThat(statusOf(seed[0])).isEqualTo("PENDING_HUMAN_APPROVAL");
        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), seed[1], Duration.ofSeconds(3))).isEmpty();
    }

    @Test
    void stateChangingEndpointsOnlyAcceptJsonSoACrossSiteFormCannotApprove() throws Exception {
        String[] seed = seedTier2();
        String approve = "/api/approvals/" + seed[0] + "/approve";
        String reject = "/api/approvals/" + seed[0] + "/reject";

        // What a forged cross-site HTML form (or a text/plain fetch) can send: never application/json.
        mvc.perform(post(approve).with(REVIEWER).contentType(MediaType.APPLICATION_FORM_URLENCODED).content("x=1"))
                .andExpect(status().isUnsupportedMediaType());
        mvc.perform(post(approve).with(REVIEWER).contentType(MediaType.TEXT_PLAIN).content("{}"))
                .andExpect(status().isUnsupportedMediaType());
        // The loophole a body-less cross-site fetch would use: no content type and no body at all.
        mvc.perform(post(approve).with(REVIEWER)).andExpect(status().isUnsupportedMediaType());
        mvc.perform(post(approve).with(REVIEWER).contentType(MediaType.APPLICATION_JSON)).andExpect(status().isBadRequest());
        mvc.perform(post(reject).with(REVIEWER).contentType(MediaType.APPLICATION_FORM_URLENCODED).content("notes=x"))
                .andExpect(status().isUnsupportedMediaType());

        assertThat(statusOf(seed[0])).isEqualTo("PENDING_HUMAN_APPROVAL");
        assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), seed[1], Duration.ofSeconds(3))).isEmpty();
    }

    @Test
    void unknownIdsAndMalformedRequestsAreHandledCleanly() throws Exception {
        mvc.perform(get("/api/approvals/" + UUID.randomUUID()).with(REVIEWER)).andExpect(status().isNotFound());
        mvc.perform(post("/api/approvals/" + UUID.randomUUID() + "/approve").with(REVIEWER).contentType(MediaType.APPLICATION_JSON).content("{}")).andExpect(status().isNotFound());
        mvc.perform(get("/api/approvals/not-a-uuid").with(REVIEWER)).andExpect(status().isBadRequest());
        mvc.perform(get("/api/approvals?status=NOPE").with(REVIEWER)).andExpect(status().isBadRequest());
    }

    @Test
    void twoReviewersApprovingAtOnceReplayExactlyOnce() throws Exception {
        String[] seed = seedTier2();
        var pool = Executors.newFixedThreadPool(2);
        try {
            List<Callable<Integer>> clicks = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                clicks.add(() -> mvc.perform(post("/api/approvals/" + seed[0] + "/approve").with(REVIEWER).contentType(MediaType.APPLICATION_JSON).content("{}"))
                        .andReturn().getResponse().getStatus());
            }
            List<Integer> codes = new ArrayList<>();
            for (var f : pool.invokeAll(clicks)) {
                codes.add(f.get());
            }

            assertThat(codes).containsExactlyInAnyOrder(200, 409);
            assertThat(MainTopicReader.read(KAFKA.getBootstrapServers(), seed[1], Duration.ofSeconds(30))).hasSize(1);
        } finally {
            pool.shutdownNow();
        }
    }
}
