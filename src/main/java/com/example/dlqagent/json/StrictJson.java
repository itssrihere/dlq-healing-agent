package com.example.dlqagent.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The one JSON dialect used for untrusted payloads: no duplicate keys, no trailing tokens, exact decimals
 * (no double rounding) and plain-notation decimals on output.
 */
public final class StrictJson {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            .build();

    private StrictJson() {}

    /** Serializes with the same settings (plain-notation decimals). The shared mapper itself is not exposed. */
    public static String write(JsonNode node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot serialize JSON: " + e.getOriginalMessage(), e);
        }
    }

    /** @throws IllegalArgumentException if the text is not a single, well-formed JSON object */
    public static ObjectNode parseObject(String json) {
        try {
            JsonNode node = MAPPER.readTree(json);
            if (node == null || !node.isObject()) {
                throw new IllegalArgumentException("not a JSON object");
            }
            return (ObjectNode) node;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("invalid JSON: " + e.getOriginalMessage(), e);
        }
    }
}
