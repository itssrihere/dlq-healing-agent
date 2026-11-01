package com.example.dlqagent.agent;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.domain.FieldChange;
import com.example.dlqagent.domain.RepairResult;
import com.example.dlqagent.contract.SchemaDrivenTopicAdapter;
import com.example.dlqagent.support.OrdersContract;
import com.example.dlqagent.support.TestProps;
import com.example.dlqagent.support.ScriptedChatModel;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.core.io.ClassPathResource;

class SchemaRepairAgentTest {

    private static final String DRIFTED = """
            {"orderId":"o-1","customerId":"c-1","amount":"19.99 USD","currency":"USD","status":"PAID","createdAt":"2025-04-03T10:00:00Z"}""";

    private final ScriptedChatModel model = ScriptedChatModel.replying("unset");
    private final SchemaDrivenTopicAdapter adapter = OrdersContract.adapter();
    private final SchemaRepairAgent agent = new SchemaRepairAgent(
            ChatClient.builder(model),
            new TopicAdapterRegistry(List.of(adapter)),
            new PatchApplier(),
            TestProps.defaults(),
            new ClassPathResource("prompts/repair-system.txt"));

    private static FailureContext failure(String payload) {
        return new FailureContext("cid-1", "orders-dlq", 0, 1L, "orders-topic", "o-1", payload, Map.of(),
                "ConstraintViolation: amount", Instant.now());
    }

    private RepairResult repairWith(String payload, String modelReply) {
        model.respondWith(p -> modelReply);
        return agent.repair(failure(payload));
    }

    private static String proposal(boolean repairable, String summary, String edits, String drop) {
        return proposal(repairable, 0.95, summary, edits, drop);
    }

    private static String proposal(boolean repairable, double confidence, String summary, String edits, String drop) {
        return "{\"repairable\":%s,\"confidence\":%s,\"summary\":\"%s\",\"edits\":%s,\"dropFields\":%s}"
                .formatted(repairable, confidence, summary, edits, drop);
    }

    @Test
    void validPatchPassesTheGateAndIsReturned() {
        var result = repairWith(DRIFTED, proposal(true, "amount text to number",
                "[{\"field\":\"amount\",\"newValue\":\"19.99\"}]", "[]"));

        assertThat(result).isInstanceOfSatisfying(RepairResult.Repaired.class, r -> {
            assertThat(r.summary()).isEqualTo("amount text to number");
            assertThat(r.riskFlags()).isEmpty(); // a purely mechanical fix: Tier 1 material
            assertThat(r.confidence()).isEqualTo(0.95);
            assertThat(adapter.evaluate(DRIFTED, r.payload(), "o-1").violations()).isEmpty();
            assertThat(r.payload()).contains("\"amount\":19.99").contains("\"orderId\":\"o-1\"");
        });
    }

    @Test
    void renameRepairDropsOldFieldAndSetsNewOne() {
        String renamed = DRIFTED.replace("\"amount\":\"19.99 USD\"", "\"total\":19.99");

        var result = repairWith(renamed, proposal(true, "total renamed to amount",
                "[{\"field\":\"amount\",\"newValue\":\"19.99\"}]", "[\"total\"]"));

        assertThat(result).isInstanceOfSatisfying(RepairResult.Repaired.class,
                r -> assertThat(r.riskFlags()).anyMatch(f -> f.contains("renamed"))); // valid, but a human should confirm
    }

    @Test
    void hallucinatedAmountIsStoppedByTheGate() {
        var result = repairWith(DRIFTED, proposal(true, "fixed",
                "[{\"field\":\"amount\",\"newValue\":\"29.99\"}]", "[]"));

        assertThat(result).isInstanceOfSatisfying(RepairResult.Unrepairable.class,
                u -> assertThat(u.reason()).contains("Evaluator gate rejected", "amount value changed"));
    }

    @Test
    void guessedAmbiguousDateIsRepairedButFlaggedForAHuman() {
        // Numeric amount, so the date is the only thing wrong with the payload.
        String ambiguous = DRIFTED.replace("\"19.99 USD\"", "19.99").replace("2025-04-03T10:00:00Z", "03/04/2025");

        var result = repairWith(ambiguous, proposal(true, "assumed day/month",
                "[{\"field\":\"createdAt\",\"newValue\":\"2025-04-03T00:00:00Z\"}]", "[]"));

        assertThat(result).isInstanceOfSatisfying(RepairResult.Repaired.class,
                r -> assertThat(r.riskFlags()).anyMatch(f -> f.contains("ambiguous"))
                        .anyMatch(f -> f.contains("time of day and zone were assumed")));
    }

    @Test
    void invalidConfidenceIsTreatedAsZeroAndFlagged() {
        var result = repairWith(DRIFTED, proposal(true, 7.5, "fixed",
                "[{\"field\":\"amount\",\"newValue\":\"19.99\"}]", "[]"));

        assertThat(result).isInstanceOfSatisfying(RepairResult.Repaired.class, r -> {
            assertThat(r.confidence()).isZero();
            assertThat(r.riskFlags()).anyMatch(f -> f.contains("confidence was invalid"));
        });
    }

    @Test
    void omittedConfidenceDefaultsToZero() {
        var result = repairWith(DRIFTED, "{\"repairable\":true,\"summary\":\"s\",\"edits\":"
                + "[{\"field\":\"amount\",\"newValue\":\"19.99\"}],\"dropFields\":[]}");

        assertThat(result).isInstanceOfSatisfying(RepairResult.Repaired.class, r -> assertThat(r.confidence()).isZero());
    }

    @Test
    void modelThatTriesToEditIdentityFieldIsRejected() {
        var result = repairWith(DRIFTED, proposal(true, "changed id",
                "[{\"field\":\"orderId\",\"newValue\":\"o-999\"}]", "[]"));

        assertThat(result).isInstanceOfSatisfying(RepairResult.Unrepairable.class,
                u -> assertThat(u.reason()).contains("Patch rejected", "not editable"));
    }

    @Test
    void topicWithoutAnAdapterCannotBeRepairedAndNeverReachesTheModel() {
        model.respondWith(p -> {
            throw new AssertionError("model must not be called");
        });
        var other = new FailureContext("cid-1", "orders-dlq", 0, 1L, "payments-topic", "k", DRIFTED, Map.of(), null,
                Instant.now());

        assertThat(agent.repair(other)).isInstanceOfSatisfying(RepairResult.Unrepairable.class,
                u -> assertThat(u.reason()).contains("No topic adapter", "payments-topic"));
        assertThat(model.prompts()).isEmpty();
    }

    @Test
    void adapterContractAndToolsAreSuppliedToTheModelPerRequest() {
        model.respondWith(p -> proposal(false, "x", "[]", "[]"));

        agent.repair(failure(DRIFTED));

        String system = ScriptedChatModel.systemText(model.lastPrompt());
        assertThat(system).contains("repair agent", "TOPIC CONTRACT for orders-topic", "EDITABLE FIELDS");
    }

    @Test
    void bothToolDefinitionsAreAttachedToEveryModelRequest() {
        model.respondWith(p -> proposal(false, "x", "[]", "[]"));

        agent.repair(failure(DRIFTED));

        // The contract text reaching the model proves nothing about the tools: check the request options themselves.
        var options = (org.springframework.ai.model.tool.ToolCallingChatOptions) model.lastPrompt().getOptions();
        assertThat(options.getToolCallbacks().stream().map(t -> t.getToolDefinition().name()))
                .containsExactlyInAnyOrder("describeTable", "validatePayload");
    }

    @Test
    void droppingIdentityFieldIsRejected() {
        var result = repairWith(DRIFTED, proposal(true, "x", "[]", "[\"customerId\"]"));

        assertThat(result).isInstanceOfSatisfying(RepairResult.Unrepairable.class,
                u -> assertThat(u.reason()).contains("Patch rejected"));
    }

    @Test
    void declinedProposalIsUnrepairable() {
        var result = repairWith(DRIFTED, proposal(false, "needs a human", "[]", "[]"));

        assertThat(result).isInstanceOfSatisfying(RepairResult.Unrepairable.class,
                u -> assertThat(u.reason()).contains("declined", "needs a human"));
    }

    @Test
    void rejectedPatchStillReportsWhatWasAttemptedForTheAuditTrail() {
        var result = repairWith(DRIFTED, proposal(true, 0.85, "changed the amount",
                "[{\"field\":\"amount\",\"newValue\":\"29.99\"}]", "[]"));

        assertThat(result).isInstanceOfSatisfying(RepairResult.Unrepairable.class, u -> {
            assertThat(u.summary()).isEqualTo("changed the amount");
            assertThat(u.confidence()).isEqualTo(0.85);
            assertThat(u.attempted()).anyMatch(c -> c.field().equals("amount") && c.after().equals("29.99"));
        });
    }

    @Test
    void repairedResultCarriesTheFieldLevelDiff() {
        var result = repairWith(DRIFTED, proposal(true, "amount text to number",
                "[{\"field\":\"amount\",\"newValue\":\"19.99\"}]", "[]"));

        assertThat(result).isInstanceOfSatisfying(RepairResult.Repaired.class, r ->
                assertThat(r.changes()).containsExactly(new FieldChange("amount", "\"19.99 USD\"", "19.99")));
    }

    @Test
    void emptyPatchIsNotARepair() {
        var result = repairWith(DRIFTED, proposal(true, "nothing", "[]", "[]"));

        assertThat(result).isInstanceOfSatisfying(RepairResult.Unrepairable.class,
                u -> assertThat(u.reason()).contains("changes nothing"));
    }

    @Test
    void nonJsonPayloadIsNeverSentToTheModel() {
        model.respondWith(p -> {
            throw new AssertionError("model must not be called");
        });

        assertThat(agent.repair(failure("\u0000 garbage"))).isInstanceOf(RepairResult.Unrepairable.class);
        assertThat(model.prompts()).isEmpty();
    }

    @Test
    void garbageModelReplyAndModelErrorsAreUnrepairable() {
        assertThat(repairWith(DRIFTED, "I fixed it!")).isInstanceOf(RepairResult.Unrepairable.class);

        model.respondWith(p -> {
            throw new IllegalStateException("timeout");
        });
        assertThat(agent.repair(failure(DRIFTED))).isInstanceOfSatisfying(RepairResult.Unrepairable.class,
                u -> assertThat(u.reason()).contains("IllegalStateException"));
    }

    @Test
    void promptCarriesKeyPayloadAndStackTraceInsideTheFence() {
        model.respondWith(p -> proposal(false, "x", "[]", "[]"));

        agent.repair(failure(DRIFTED));

        String user = model.lastPrompt().getUserMessage().getText();
        assertThat(user).contains("kafka key: o-1", "19.99 USD", "ConstraintViolation: amount", "untrusted");
        assertThat(ScriptedChatModel.isRepairer(model.lastPrompt())).isTrue();
    }
}
