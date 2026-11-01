package com.example.dlqagent.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.dlqagent.domain.TopicAdapter.FieldType;
import com.example.dlqagent.support.OrdersContract;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SchemaDrivenTopicAdapterTest {

    private final SchemaDrivenTopicAdapter adapter = OrdersContract.adapter();

    @Test
    void identityFieldsAreNeverEditable() {
        assertThat(adapter.editableFields().keySet()).doesNotContainAnyElementsOf(adapter.immutableFields());
        assertThat(adapter.immutableFields()).containsExactlyInAnyOrder("orderId", "customerId");
    }

    @Test
    void editableFieldsAreDerivedFromTheSchemaTypes() {
        // exactly what the hand-written allowlist used to say, now read from the schema
        assertThat(adapter.editableFields()).containsOnly(
                java.util.Map.entry("amount", FieldType.DECIMAL), java.util.Map.entry("currency", FieldType.TEXT),
                java.util.Map.entry("status", FieldType.TEXT), java.util.Map.entry("createdAt", FieldType.TEXT));
    }

    @Test
    void promptCarriesTheSchemaAndTheEnforcedFieldSets() {
        assertThat(adapter.contract()).startsWith("TOPIC CONTRACT for orders-topic")
                .contains("\"additionalProperties\":false", "\"exclusiveMinimum\":0", "\"SHIPPED\"")
                .contains("EDITABLE FIELDS", "amount (DECIMAL)", "createdAt (TEXT)", "currency (TEXT)", "status (TEXT)")
                .contains("IDENTITY FIELDS", "customerId, orderId")
                .contains("KEY: orderId must equal the Kafka key", "NOT IN THE FUTURE: createdAt")
                .contains("describeTable", "validatePayload");
    }

    @Test
    void servesItsTopicAndExposesOneToolObject() {
        assertThat(adapter.topic()).isEqualTo("orders-topic");
        assertThat(adapter.tools()).hasSize(1).first().isInstanceOf(ContractTools.class);
    }

    @Test
    void aNewFieldInTheSchemaNeedsNoCodeToBecomeEditable() {
        String schema = """
                {"type":"object","additionalProperties":false,"required":["id"],
                 "properties":{"id":{"type":"string"},"note":{"type":"string"},"qty":{"type":"integer"},
                               "flag":{"type":"boolean"},"tags":{"type":"array","items":{"type":"string"}}}}""";
        var contract = new SchemaContract(schema, new ContractPolicy(Set.of("id"), "id", Set.of()), OrdersContract.CLOCK);

        assertThat(contract.editableFields()).containsOnlyKeys("note", "qty"); // scalars only; never identity, boolean, array
        assertThat(contract.editableFields()).containsEntry("qty", FieldType.DECIMAL);
    }

    @Test
    void aPolicyThatDisagreesWithTheSchemaStopsTheServiceAtStartUp() {
        String schema = """
                {"type":"object","properties":{"id":{"type":"string"},"at":{"type":"string"}}}""";

        assertThatThrownBy(() -> new SchemaContract(schema, new ContractPolicy(Set.of("nope"), null, Set.of()), OrdersContract.CLOCK))
                .hasMessageContaining("identity field 'nope'");
        assertThatThrownBy(() -> new SchemaContract(schema, new ContractPolicy(Set.of("id"), "missing", Set.of()), OrdersContract.CLOCK))
                .hasMessageContaining("key field 'missing'");
        assertThatThrownBy(() -> new SchemaContract(schema, new ContractPolicy(Set.of("id"), null, Set.of("at")), OrdersContract.CLOCK))
                .hasMessageContaining("no-future field 'at'");
        assertThatThrownBy(() -> new SchemaContract("[1]", new ContractPolicy(Set.of(), null, Set.of()), OrdersContract.CLOCK))
                .hasMessageContaining("type object with properties");
        assertThatThrownBy(() -> new SchemaContract("{", new ContractPolicy(Set.of(), null, Set.of()), OrdersContract.CLOCK))
                .hasMessageContaining("not valid JSON");
    }
}
