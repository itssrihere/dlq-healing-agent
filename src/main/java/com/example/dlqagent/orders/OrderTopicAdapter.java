package com.example.dlqagent.orders;

import com.example.dlqagent.domain.Evaluation;
import com.example.dlqagent.domain.TopicAdapter;
import com.example.dlqagent.infrastructure.DlqAgentProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

/** Adapter for the orders topic: the {@link OrderEvent} contract, its two read-only tools and its rules. */
@Component
public class OrderTopicAdapter implements TopicAdapter {

    private static final Map<String, FieldType> EDITABLE = Map.of(
            "amount", FieldType.DECIMAL,
            "currency", FieldType.TEXT,
            "status", FieldType.TEXT,
            "createdAt", FieldType.TEXT);

    private final String topic;
    private final OrderContractValidator validator;
    private final OrderSchemaTools tools;
    private final String contract;

    @Autowired
    OrderTopicAdapter(DlqAgentProperties props, OrderContractValidator validator, OrderSchemaTools tools,
                      @Value("classpath:prompts/orders-contract.txt") Resource contractText) {
        this(props.topics().main(), validator, tools, read(contractText));
    }

    public OrderTopicAdapter(String topic, OrderContractValidator validator, OrderSchemaTools tools,
                             String contractText) {
        this.topic = topic;
        this.validator = validator;
        this.tools = tools;
        // The editable and identity field lists are generated from the same sets the engine enforces,
        // so the prompt can never promise the model an edit the applier would refuse.
        this.contract = contractText + "\n\nEDITABLE FIELDS (the only fields you may edit): "
                + EDITABLE.entrySet().stream().sorted(Map.Entry.comparingByKey())
                        .map(e -> e.getKey() + " (" + e.getValue() + ")").collect(Collectors.joining(", "))
                + "\nIDENTITY FIELDS (never editable, never droppable): "
                + OrderContractValidator.IMMUTABLE_FIELDS.stream().sorted().collect(Collectors.joining(", "));
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public String contract() {
        return contract;
    }

    @Override
    public Map<String, FieldType> editableFields() {
        return EDITABLE;
    }

    @Override
    public Set<String> immutableFields() {
        return OrderContractValidator.IMMUTABLE_FIELDS;
    }

    @Override
    public List<String> validate(String json) {
        return validator.validate(json);
    }

    @Override
    public Evaluation evaluate(String originalJson, String candidateJson, String kafkaKey) {
        EvaluationResult r = validator.evaluate(originalJson, candidateJson, kafkaKey);
        return new Evaluation(r.violations(), r.riskFlags());
    }

    @Override
    public List<Object> tools() {
        return List.of(tools);
    }

    private static String read(Resource resource) {
        try {
            return resource.getContentAsString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read order contract text", e);
        }
    }
}
