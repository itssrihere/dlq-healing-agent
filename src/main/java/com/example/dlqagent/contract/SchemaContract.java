package com.example.dlqagent.contract;

import com.example.dlqagent.domain.TopicAdapter.FieldType;
import com.example.dlqagent.json.StrictJson;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A topic's contract: a JSON Schema document and the {@link ContractPolicy}. Checked when it is loaded, so a
 * contract that does not make sense (a policy naming a field the schema does not have) stops the service at start-up
 * instead of surfacing on the first failed event.
 */
public final class SchemaContract {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_ERRORS = 20;
    private static final Duration CLOCK_SKEW = Duration.ofMinutes(5);

    private final JsonNode document;
    private final Schema schema;
    private final Map<String, FieldSpec> fields;
    private final ContractPolicy policy;
    private final Clock clock;

    public SchemaContract(String schemaText, ContractPolicy policy, Clock clock) {
        this.policy = policy;
        this.clock = clock;
        try {
            this.document = MAPPER.readTree(schemaText);
        } catch (Exception e) {
            throw new IllegalArgumentException("contract is not valid JSON: " + e.getMessage(), e);
        }
        if (document == null || !document.isObject() || !"object".equals(document.path("type").asText())
                || !document.path("properties").isObject()) {
            throw new IllegalArgumentException("contract must be a JSON Schema of type object with properties");
        }
        this.fields = readFields(document.get("properties"));
        checkPolicy();
        // The library's own "format" assertions stay off: its RFC 3339 check is stricter than the Java parser the
        // consumer actually uses (it rejects the valid offset -00:00). Date and date-time properties are checked
        // below, generically, with the consumer's parser, so "format" is still enforced rather than decorative.
        SchemaRegistry registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12,
                builder -> builder.schemaRegistryConfig(
                        SchemaRegistryConfig.builder().formatAssertionsEnabled(false).build()));
        this.schema = registry.getSchema(document);
    }

    public ContractPolicy policy() {
        return policy;
    }

    Map<String, FieldSpec> fields() {
        return fields;
    }

    /** The schema document, compact, as shown to the model. */
    public String schemaText() {
        return document.toString();
    }

    /**
     * Violations of the shape contract. Also rejects control characters in any string: a regex's {@code $} can match
     * before a trailing newline, and a newline in an identifier or enum value is never legitimate.
     */
    public List<String> violations(ObjectNode payload) {
        List<String> found = new ArrayList<>();
        List<String> messages = new ArrayList<>();
        for (Error error : schema.validate(payload)) {
            messages.add(describe(error));
        }
        messages.stream().sorted().limit(MAX_ERRORS).forEach(found::add);
        payload.fields().forEachRemaining(e -> {
            if (e.getValue().isTextual() && hasControlCharacter(e.getValue().asText())) {
                found.add(e.getKey() + " contains control characters");
            }
        });
        fields.values().stream().filter(FieldSpec::isDateTime).forEach(f -> {
            JsonNode value = payload.get(f.name());
            if (value != null && value.isTextual()) {
                checkDate(f, value.asText(), found);
            }
        });
        return found;
    }

    /** The library's message alone does not say which field failed ("does not match the regex pattern ..."). */
    private static String describe(Error error) {
        String pointer = String.valueOf(error.getInstanceLocation()); // a JSON pointer such as /amount
        String field = pointer.startsWith("/") ? pointer.substring(1).replace('/', '.') : pointer;
        return field.isEmpty() ? error.getMessage() : field + " " + error.getMessage();
    }

    private void checkDate(FieldSpec field, String text, List<String> found) {
        try {
            if ("date".equals(field.format())) {
                LocalDate.parse(text);
                return;
            }
            OffsetDateTime parsed = OffsetDateTime.parse(text);
            if (policy.noFutureFields().contains(field.name())
                    && parsed.toInstant().isAfter(clock.instant().plus(CLOCK_SKEW))) {
                found.add(field.name() + " is in the future");
            }
        } catch (DateTimeParseException e) {
            found.add(field.name() + " must be an ISO-8601 " + field.format() + " with an offset where one applies");
        }
    }

    /**
     * Fields a patch may set: every plain string or number property that is not an identity field. Nested objects,
     * arrays and anything unusual are never editable.
     */
    public Map<String, FieldType> editableFields() {
        Map<String, FieldType> editable = new TreeMap<>();
        fields.values().forEach(f -> {
            if (!policy.identityFields().contains(f.name())) {
                switch (f.kind()) {
                    case STRING -> editable.put(f.name(), FieldType.TEXT);
                    case NUMBER, INTEGER -> editable.put(f.name(), FieldType.DECIMAL);
                    case OTHER -> { /* not editable */ }
                }
            }
        });
        return Map.copyOf(editable);
    }

    private static Map<String, FieldSpec> readFields(JsonNode properties) {
        Map<String, FieldSpec> found = new LinkedHashMap<>();
        properties.fields().forEachRemaining(e -> {
            JsonNode def = e.getValue();
            FieldSpec.Kind kind = def.path("type").isTextual() ? switch (def.get("type").asText()) {
                case "string" -> FieldSpec.Kind.STRING;
                case "number" -> FieldSpec.Kind.NUMBER;
                case "integer" -> FieldSpec.Kind.INTEGER;
                default -> FieldSpec.Kind.OTHER;
            } : FieldSpec.Kind.OTHER;
            String format = def.path("format").isTextual() ? def.get("format").asText() : null;
            found.put(e.getKey(), new FieldSpec(e.getKey(), kind, format));
        });
        return java.util.Collections.unmodifiableMap(found); // keeps the schema's own field order
    }

    private void checkPolicy() {
        for (String f : policy.identityFields()) {
            require(fields.containsKey(f), "identity field '" + f + "' is not a property of the contract");
        }
        if (policy.keyField() != null) {
            require(fields.containsKey(policy.keyField()), "key field '" + policy.keyField() + "' is not a property of the contract");
        }
        for (String f : policy.noFutureFields()) {
            require(fields.containsKey(f) && fields.get(f).isDateTime(),
                    "no-future field '" + f + "' must be a date-time property of the contract");
        }
    }

    private static void require(boolean ok, String message) {
        if (!ok) {
            throw new IllegalArgumentException("invalid contract policy: " + message);
        }
    }

    private static boolean hasControlCharacter(String s) {
        return s.chars().anyMatch(c -> c < 0x20 || c == 0x7f);
    }

    /** Parses with the shared strict dialect. @throws IllegalArgumentException if not a single JSON object */
    public static ObjectNode parse(String json) {
        return StrictJson.parseObject(json);
    }
}
