package com.example.dlqagent.orders;

import com.example.dlqagent.domain.SchemaInspector;
import java.util.List;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;

/**
 * The only tools the repair agent can call. Both are read-only, take typed parameters and
 * delegate to ports; the model never receives SQL, Kafka or write access.
 */
@Component
public class OrderSchemaTools {

    public record ValidationReport(boolean valid, List<String> violations) {}

    private final SchemaInspector inspector;
    private final OrderContractValidator validator;

    public OrderSchemaTools(SchemaInspector inspector, OrderContractValidator validator) {
        this.inspector = inspector;
        this.validator = validator;
    }

    @Tool(description = "Describe a database table: columns, types, nullability, lengths and CHECK/PRIMARY KEY "
            + "constraints. Read-only. The only allowed table is 'orders'.")
    public SchemaInspector.TableSchema describeTable(
            @ToolParam(description = "Table name; only 'orders' is allowed") String tableName) {
        return inspector.describe(tableName);
    }

    @Tool(description = "Check a complete order event JSON against the order schema and business rules. "
            + "Read-only. Returns the list of violations; an empty list means the payload is valid.")
    public ValidationReport validateOrderPayload(
            @ToolParam(description = "The complete order event as a JSON string") String json) {
        List<String> violations = validator.validate(json);
        return new ValidationReport(violations.isEmpty(), violations);
    }
}
