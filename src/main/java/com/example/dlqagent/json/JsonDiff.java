package com.example.dlqagent.json;

import com.example.dlqagent.domain.FieldChange;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Field-level (top-level keys) diff of two JSON objects, for human review and for the audit trail. */
public final class JsonDiff {

    private JsonDiff() {}

    public static List<FieldChange> diff(ObjectNode before, ObjectNode after) {
        Set<String> fields = new LinkedHashSet<>();
        before.fieldNames().forEachRemaining(fields::add);
        after.fieldNames().forEachRemaining(fields::add);

        List<FieldChange> changes = new ArrayList<>();
        for (String f : fields) {
            JsonNode b = before.get(f);
            JsonNode a = after.get(f);
            if (b == null || a == null || !b.equals(a)) {
                changes.add(new FieldChange(f, b == null ? null : b.toString(), a == null ? null : a.toString()));
            }
        }
        return changes;
    }

    /** Like {@link #diff(ObjectNode, ObjectNode)} but returns an empty list if either side is not a JSON object. */
    public static List<FieldChange> diffOrEmpty(String before, String after) {
        try {
            return diff(StrictJson.parseObject(before), StrictJson.parseObject(after));
        } catch (IllegalArgumentException | NullPointerException e) {
            return List.of();
        }
    }
}
