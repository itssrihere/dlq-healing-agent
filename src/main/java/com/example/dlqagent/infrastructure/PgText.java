package com.example.dlqagent.infrastructure;

import com.example.dlqagent.domain.FieldChange;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Postgres text and jsonb cannot hold NUL (U+0000). Hostile payloads can contain one, and it must never make a
 * quarantine or audit insert fail forever, so it is replaced with U+FFFD.
 */
final class PgText {

    private PgText() {}

    static String clean(String value) {
        return value == null ? null : value.replace('\u0000', '�');
    }

    static Map<String, String> cleanHeaders(Map<String, String> headers) {
        Map<String, String> cleaned = new LinkedHashMap<>();
        headers.forEach((k, v) -> cleaned.put(clean(k), clean(v)));
        return cleaned;
    }

    static List<String> cleanAll(List<String> values) {
        return values.stream().map(PgText::clean).toList();
    }

    static List<FieldChange> cleanChanges(List<FieldChange> changes) {
        return changes.stream()
                .map(c -> new FieldChange(clean(c.field()), clean(c.before()), clean(c.after())))
                .toList();
    }
}
