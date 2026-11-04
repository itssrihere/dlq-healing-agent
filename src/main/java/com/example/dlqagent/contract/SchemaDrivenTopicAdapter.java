package com.example.dlqagent.contract;

import com.example.dlqagent.domain.Evaluation;
import com.example.dlqagent.domain.SchemaInspector;
import com.example.dlqagent.domain.TopicAdapter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link TopicAdapter} built entirely from a {@link SchemaContract}: the prompt, the editable fields, the
 * validation tool and the evaluator gate are derived from the schema and the small policy. A new topic is a new
 * schema file and a few lines of configuration, with no new Java.
 */
public final class SchemaDrivenTopicAdapter implements TopicAdapter {

    private final String topic;
    private final SchemaContract contract;
    private final Map<String, FieldType> editable;
    private final ChangeRules rules;
    private final ContractTools tools;
    private final String prompt;

    public SchemaDrivenTopicAdapter(String topic, SchemaContract contract, SchemaInspector inspector,
                                    Set<String> tables) {
        this.topic = topic;
        this.contract = contract;
        this.editable = contract.editableFields();
        this.rules = new ChangeRules(contract);
        this.tools = new ContractTools(inspector, this, tables);
        this.prompt = prompt(topic, contract, editable);
    }

    private static String prompt(String topic, SchemaContract contract, Map<String, FieldType> editable) {
        ContractPolicy policy = contract.policy();
        StringBuilder text = new StringBuilder()
                .append("TOPIC CONTRACT for ").append(topic).append('\n')
                .append("Each event is one JSON object that must satisfy this JSON Schema. The schema is the "
                        + "authority; its descriptions explain intent.\n")
                .append(contract.schemaText()).append("\n\n")
                // Generated from the same sets the engine enforces, so the prompt can never promise the model an
                // edit the applier would refuse.
                .append("EDITABLE FIELDS (the only fields you may edit): ")
                .append(editable.entrySet().stream().sorted(Map.Entry.comparingByKey())
                        .map(e -> e.getKey() + " (" + e.getValue() + ")").collect(Collectors.joining(", ")))
                .append("\nIDENTITY FIELDS (never editable, never droppable): ")
                .append(policy.identityFields().stream().sorted().collect(Collectors.joining(", ")));
        if (policy.keyField() != null) {
            text.append("\nKEY: ").append(policy.keyField()).append(" must equal the Kafka key.");
        }
        if (!policy.noFutureFields().isEmpty()) {
            text.append("\nNOT IN THE FUTURE: ")
                    .append(policy.noFutureFields().stream().sorted().collect(Collectors.joining(", ")));
        }
        return text.append("\n\nTOOLS FOR THIS TOPIC\n")
                .append("- describeTable: read a database table definition (column types, lengths, CHECK constraints).\n")
                .append("- validatePayload: check a complete event JSON against the contract above and get the "
                        + "list of violations.")
                .toString();
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public String contract() {
        return prompt;
    }

    @Override
    public Map<String, FieldType> editableFields() {
        return Map.copyOf(editable); // already immutable, so this returns the same instance; it just keeps the field private
    }

    @Override
    public Set<String> immutableFields() {
        return contract.policy().identityFields();
    }

    @Override
    public List<String> validate(String json) {
        ObjectNode node;
        try {
            node = SchemaContract.parse(json);
        } catch (IllegalArgumentException e) {
            return List.of("payload is " + e.getMessage());
        }
        return contract.violations(node);
    }

    @Override
    public Evaluation evaluate(String originalJson, String candidateJson, String kafkaKey) {
        ObjectNode candidate;
        try {
            candidate = SchemaContract.parse(candidateJson);
        } catch (IllegalArgumentException e) {
            return new Evaluation(List.of("candidate is " + e.getMessage()), List.of());
        }
        ObjectNode original;
        try {
            original = SchemaContract.parse(originalJson);
        } catch (IllegalArgumentException e) {
            return new Evaluation(List.of("original is " + e.getMessage() + "; identity fields cannot be verified"),
                    List.of());
        }
        return rules.evaluate(original, candidate, kafkaKey);
    }

    @Override
    public List<Object> tools() {
        return List.of(tools);
    }
}
