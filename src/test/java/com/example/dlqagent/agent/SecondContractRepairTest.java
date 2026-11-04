package com.example.dlqagent.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.example.dlqagent.contract.ContractPolicy;
import com.example.dlqagent.contract.SchemaContract;
import com.example.dlqagent.contract.SchemaDrivenTopicAdapter;
import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.domain.RepairResult;
import com.example.dlqagent.domain.SchemaInspector;
import com.example.dlqagent.domain.TopicAdapter.FieldType;
import com.example.dlqagent.support.OrdersContract;
import com.example.dlqagent.support.ScriptedChatModel;
import com.example.dlqagent.support.TestProps;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.core.io.ClassPathResource;

/**
 * The claim "a new topic is a schema file and a few lines of configuration, no new Java" as a test. A second contract
 * with different field types (an integer, an enum, a plain date) goes through the real repair agent, patch applier
 * and gate. Nothing in this test, or in the main code it exercises, knows about shipments.
 */
class SecondContractRepairTest {

    private static final String TOPIC = "shipments-topic";
    private static final String DRIFTED = """
            {"shipmentId":"S-000123","orderRef":"o-1","carrier":"dhl","quantity":"3 units","shippedOn":"2025-04-03"}""";

    private final ScriptedChatModel model = ScriptedChatModel.replying("unset");
    private final SchemaDrivenTopicAdapter adapter = shipments();
    private final SchemaRepairAgent agent = new SchemaRepairAgent(
            ChatClient.builder(model), new TopicAdapterRegistry(List.of(adapter)), new PatchApplier(),
            TestProps.defaults(), new ClassPathResource("prompts/repair-system.txt"));

    private static SchemaDrivenTopicAdapter shipments() {
        try {
            String schema = new ClassPathResource("contracts/shipments.schema.json").getContentAsString(StandardCharsets.UTF_8);
            return new SchemaDrivenTopicAdapter(TOPIC,
                    new SchemaContract(schema, new ContractPolicy(Set.of("shipmentId", "orderRef"), "shipmentId", Set.of()),
                            OrdersContract.CLOCK),
                    mock(SchemaInspector.class), Set.of());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private RepairResult repair(String payload, String edits) {
        model.respondWith(p -> "{\"repairable\":true,\"confidence\":0.95,\"summary\":\"fix\",\"edits\":%s,\"dropFields\":[]}"
                .formatted(edits));
        return agent.repair(new FailureContext("orders", "cid", "shipments-dlq", 0, 1L, TOPIC, "S-000123", payload, Map.of(),
                "ConstraintViolation: quantity", Instant.now()));
    }

    @Test
    void editableFieldsAndPromptAreDerivedFromTheShipmentsSchema() {
        assertThat(adapter.editableFields()).containsOnly(Map.entry("carrier", FieldType.TEXT),
                Map.entry("quantity", FieldType.DECIMAL), Map.entry("shippedOn", FieldType.TEXT));
        assertThat(adapter.immutableFields()).containsExactlyInAnyOrder("shipmentId", "orderRef");
        assertThat(adapter.contract()).contains("TOPIC CONTRACT for shipments-topic", "\"FEDEX\"", "quantity (DECIMAL)",
                "KEY: shipmentId must equal the Kafka key");
    }

    @Test
    void aTextQuantityBecomesTheSameWholeNumberWithNoRiskFlag() {
        var result = repair(DRIFTED, "[{\"field\":\"quantity\",\"newValue\":\"3\"},{\"field\":\"carrier\",\"newValue\":\"DHL\"}]");

        assertThat(result).isInstanceOfSatisfying(RepairResult.Repaired.class, r -> {
            assertThat(r.payload()).contains("\"quantity\":3").contains("\"carrier\":\"DHL\"");
            assertThat(r.riskFlags()).isEmpty(); // a number rewritten, a case normalised: mechanical, so Tier 1 material
        });
    }

    @Test
    void aChangedQuantityValueIsRejectedByTheGate() {
        var result = repair(DRIFTED, "[{\"field\":\"quantity\",\"newValue\":\"4\"}]");

        assertThat(result).isInstanceOfSatisfying(RepairResult.Unrepairable.class,
                u -> assertThat(u.reason()).contains("quantity value changed from 3 to 4"));
    }

    @Test
    void aNonIntegerQuantityFailsTheSchema() {
        var result = repair(DRIFTED, "[{\"field\":\"quantity\",\"newValue\":\"3.5\"}]");

        assertThat(result).isInstanceOfSatisfying(RepairResult.Unrepairable.class,
                u -> assertThat(u.reason()).contains("quantity"));
    }

    @Test
    void mappingACarrierNameToAnotherValueIsFlaggedForAHuman() {
        String payload = DRIFTED.replace("\"dhl\"", "\"Deutsche Post\"").replace("\"3 units\"", "3");
        var result = repair(payload, "[{\"field\":\"carrier\",\"newValue\":\"DHL\"}]");

        assertThat(result).isInstanceOfSatisfying(RepairResult.Repaired.class,
                r -> assertThat(r.riskFlags()).anyMatch(f -> f.contains("carrier mapped from 'Deutsche Post' to 'DHL'")));
    }

    @Test
    void identityFieldsAreProtectedWithoutAnyShipmentSpecificCode() {
        var result = repair(DRIFTED, "[{\"field\":\"shipmentId\",\"newValue\":\"S-999999\"}]");

        assertThat(result).isInstanceOfSatisfying(RepairResult.Unrepairable.class,
                u -> assertThat(u.reason()).contains("shipmentId' is not editable"));
    }

    @Test
    void anAmbiguousPlainDateIsFlaggedAndNeverClaimsATimeWasAssumed() {
        String payload = DRIFTED.replace("\"dhl\"", "\"DHL\"").replace("\"3 units\"", "3").replace("2025-04-03", "03/04/2025");
        var result = repair(payload, "[{\"field\":\"shippedOn\",\"newValue\":\"2025-04-03\"}]");

        assertThat(result).isInstanceOfSatisfying(RepairResult.Repaired.class, r -> {
            assertThat(r.riskFlags()).anyMatch(f -> f.contains("ambiguous"));
            assertThat(r.riskFlags()).noneMatch(f -> f.contains("time of day"));
        });
    }

    @Test
    void theKeyRuleComesFromThePolicy() {
        assertThat(adapter.evaluate(DRIFTED.replace("\"3 units\"", "3").replace("\"dhl\"", "\"DHL\""),
                DRIFTED.replace("\"3 units\"", "3").replace("\"dhl\"", "\"DHL\""), "other-key").violations())
                .anyMatch(v -> v.contains("shipmentId must equal the Kafka key"));
    }
}
