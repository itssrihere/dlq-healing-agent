package com.example.dlqagent.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.example.dlqagent.domain.SchemaInspector;
import com.example.dlqagent.support.OrdersContract;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.ai.support.ToolCallbacks;
import org.springframework.ai.tool.ToolCallback;

class ContractToolsTest {

    private final SchemaInspector inspector = mock(SchemaInspector.class);
    private final ContractTools tools = new ContractTools(inspector,
            new SchemaDrivenTopicAdapter("orders-topic",
                    new SchemaContract(OrdersContract.schema(), OrdersContract.policy(), OrdersContract.CLOCK), inspector,
                    java.util.Set.of("orders")),
            java.util.Set.of("orders"));

    @Test
    void exposesExactlyTwoReadOnlyTools() {
        ToolCallback[] callbacks = ToolCallbacks.from(tools);

        assertThat(Arrays.stream(callbacks).map(c -> c.getToolDefinition().name()))
                .containsExactlyInAnyOrder("describeTable", "validatePayload");
        assertThat(Arrays.stream(callbacks).map(c -> c.getToolDefinition().inputSchema()))
                .anyMatch(s -> s.contains("tableName"))
                .anyMatch(s -> s.contains("json"));
    }

    @Test
    void toolCallbackInvokesValidationWithTypedJsonArguments() {
        ToolCallback validate = Arrays.stream(ToolCallbacks.from(tools))
                .filter(c -> c.getToolDefinition().name().equals("validatePayload")).findFirst().orElseThrow();

        String result = validate.call("{\"json\": \"{\\\"orderId\\\": \\\"o-1\\\"}\"}");

        assertThat(result).contains("\"valid\":false").contains("customerId");
    }

    @Test
    void describeTableDelegatesToInspector() {
        var schema = new SchemaInspector.TableSchema("orders", List.of(), List.of("PRIMARY KEY (order_id)"));
        when(inspector.describe("orders")).thenReturn(schema);

        assertThat(tools.describeTable("orders")).isSameAs(schema);
    }

    @Test
    void aTableOutsideThisTopicsListIsRefusedBeforeTheInspectorIsAsked() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> tools.describeTable("shipments"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not allowed for this topic");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> tools.describeTable("pg_catalog.pg_user"))
                .isInstanceOf(IllegalArgumentException.class);
        org.mockito.Mockito.verifyNoInteractions(inspector);
    }

    @Test
    void tableNamesAreMatchedWithoutRegardToCaseOrPadding() {
        var schema = new SchemaInspector.TableSchema("orders", List.of(), List.of());
        when(inspector.describe("orders")).thenReturn(schema);

        assertThat(tools.describeTable("  ORDERS ")).isSameAs(schema);
    }
}
