package com.example.dlqagent.orders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.example.dlqagent.domain.SchemaInspector;
import com.example.dlqagent.domain.TopicAdapter.FieldType;
import org.junit.jupiter.api.Test;

class OrderTopicAdapterTest {

    private final OrderContractValidator validator = new OrderContractValidator();
    private final OrderSchemaTools tools = new OrderSchemaTools(mock(SchemaInspector.class), validator);
    private final OrderTopicAdapter adapter = new OrderTopicAdapter("orders-topic", validator, tools, "CONTRACT");

    @Test
    void identityFieldsAreNeverEditable() {
        assertThat(adapter.editableFields().keySet()).doesNotContainAnyElementsOf(adapter.immutableFields());
        assertThat(adapter.immutableFields()).containsExactlyInAnyOrder("orderId", "customerId");
    }

    @Test
    void editableFieldsAreTypedAndAllBelongToTheContract() {
        assertThat(adapter.editableFields()).containsEntry("amount", FieldType.DECIMAL)
                .containsEntry("createdAt", FieldType.TEXT);
        assertThat(OrderContractValidator.FIELDS).containsAll(adapter.editableFields().keySet());
    }

    @Test
    void contractPromptListsExactlyTheEnforcedFieldSets() {
        assertThat(adapter.contract()).startsWith("CONTRACT")
                .contains("EDITABLE FIELDS", "amount (DECIMAL)", "createdAt (TEXT)", "currency (TEXT)", "status (TEXT)")
                .contains("IDENTITY FIELDS", "customerId, orderId");
    }

    @Test
    void servesTheConfiguredTopicAndExposesItsTools() {
        assertThat(adapter.topic()).isEqualTo("orders-topic");
        assertThat(adapter.tools()).containsExactly(tools);
    }

    @Test
    void delegatesValidationAndEvaluationToTheOrderRules() {
        String valid = "{\"orderId\":\"o-1\",\"customerId\":\"c-1\",\"amount\":1,\"currency\":\"USD\","
                + "\"status\":\"NEW\",\"createdAt\":\"2025-04-03T10:00:00Z\"}";

        assertThat(adapter.validate(valid)).isEmpty();
        assertThat(adapter.evaluate(valid, valid, "o-1").violations()).isEmpty();
        assertThat(adapter.evaluate(valid, valid, "other-key").rejected()).isTrue();
    }
}
