package com.example.dlqagent.contract;

import com.example.dlqagent.domain.SchemaInspector;
import com.example.dlqagent.domain.TopicAdapter;
import java.util.List;
import java.util.Set;
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
    private final Set<String> tables;

    /** @param tables the only tables this topic's model may describe; the inspector enforces its own list as well */
    public ContractTools(SchemaInspector inspector, TopicAdapter adapter, Set<String> tables) {
        this.inspector = inspector;
        this.adapter = adapter;
        this.tables = Set.copyOf(tables);
    }

    @Tool(description = "Describe a database table: columns, types, nullability, lengths and CHECK/PRIMARY KEY "
            + "constraints. Read-only. Only the tables configured for this topic are available.")
    public SchemaInspector.TableSchema describeTable(
            @ToolParam(description = "Table name, for example 'orders'") String tableName) {
        String name = tableName == null ? "" : tableName.trim().toLowerCase(java.util.Locale.ROOT);
        if (!tables.contains(name)) {
            throw new IllegalArgumentException("Table not allowed for this topic: '" + tableName + "'. Allowed: " + tables);
        }
        return inspector.describe(name);
    }

    @Tool(description = "Check a complete event JSON against this topic's contract. Read-only. Returns the list of "
            + "violations; an empty list means the payload is valid.")
    public ValidationReport validatePayload(
            @ToolParam(description = "The complete event as a JSON string") String json) {
        List<String> violations = adapter.validate(json);
        return new ValidationReport(violations.isEmpty(), violations);
    }
}
