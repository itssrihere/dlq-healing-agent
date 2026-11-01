package com.example.dlqagent.contract;

import com.example.dlqagent.domain.SchemaInspector;
import com.example.dlqagent.domain.TopicAdapter;
import java.util.List;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

/**
 * The only tools the repair agent can call, for any topic. Both are read-only, take typed parameters and delegate
 * to ports; the model never receives SQL, Kafka or write access.
 */
public final class ContractTools {

    public record ValidationReport(boolean valid, List<String> violations) {

        public ValidationReport {
            violations = List.copyOf(violations);
        }
    }

    private final SchemaInspector inspector;
    private final TopicAdapter adapter;

    public ContractTools(SchemaInspector inspector, TopicAdapter adapter) {
        this.inspector = inspector;
        this.adapter = adapter;
    }

    @Tool(description = "Describe a database table: columns, types, nullability, lengths and CHECK/PRIMARY KEY "
            + "constraints. Read-only. Only the tables the service allows are available.")
    public SchemaInspector.TableSchema describeTable(
            @ToolParam(description = "Table name, for example 'orders'") String tableName) {
        return inspector.describe(tableName);
    }

    @Tool(description = "Check a complete event JSON against this topic's contract. Read-only. Returns the list of "
            + "violations; an empty list means the payload is valid.")
    public ValidationReport validatePayload(
            @ToolParam(description = "The complete event as a JSON string") String json) {
        List<String> violations = adapter.validate(json);
        return new ValidationReport(violations.isEmpty(), violations);
    }
}
