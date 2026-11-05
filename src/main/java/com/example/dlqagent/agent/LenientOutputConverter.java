package com.example.dlqagent.agent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.ai.converter.BeanOutputConverter;

/**
 * Binds a model's structured answer, with one extra chance: if the stock binding fails, read the text as a JSON tree
 * (a key that appears twice keeps its last value) and bind that.
 *
 * <p>Why: Jackson builds a record as soon as it has seen all of its fields, so a model that repeats a key after the last
 * one, for example a trailing second {@code "confidence"}, fails with "No fallback setter/field defined for creator
 * property". In an evaluation this happened in 2 of about 200 calls of one model, and each time the whole event was
 * discarded into quarantine. A repeated key says nothing about safety: the value that wins is still judged by the same
 * gate as any other. Anything that is not JSON of the right shape still fails exactly as before.
 */
final class LenientOutputConverter<T> extends BeanOutputConverter<T> {

    private static final ObjectMapper TREE = new ObjectMapper();

    private final Class<T> type;

    LenientOutputConverter(Class<T> type) {
        super(type);
        this.type = type;
    }

    @Override
    public T convert(String text) {
        try {
            return super.convert(text);
        } catch (RuntimeException stock) {
            // every way out below rethrows the original failure: it is the one that explains what the model returned
            JsonNode tree;
            try {
                tree = TREE.readTree(stripFences(text));
            } catch (JsonProcessingException notJson) {
                throw stock;
            }
            if (tree == null || !tree.isObject()) {
                throw stock;
            }
            try {
                return TREE.treeToValue(tree, type);
            } catch (JsonProcessingException wrongShape) {
                throw stock;
            }
        }
    }

    private static String stripFences(String text) {
        String t = text == null ? "" : text.strip();
        if (t.startsWith("```")) {
            int firstNewline = t.indexOf('\n');
            t = firstNewline < 0 ? "" : t.substring(firstNewline + 1);
            if (t.endsWith("```")) {
                t = t.substring(0, t.length() - 3);
            }
        }
        return t.strip();
    }
}
