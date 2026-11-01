package com.example.dlqagent.contract;

import com.example.dlqagent.domain.Evaluation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The topic-independent rules that decide whether a repair changed only how a value is written, not what it means.
 * They read the contract's field types and the policy; they know nothing about any particular topic.
 *
 * <ul>
 *   <li>Violations (never replayed): the shape contract is broken, an identity field changed, the key field no longer
 *       equals the Kafka key, a number's value changed.
 *   <li>Risk flags (a human decides): a field was added or renamed, a string was mapped to a different value, a date
 *       was reformatted from a non-ISO form, a day/month order was guessed.
 * </ul>
 */
final class ChangeRules {

    private static final Pattern PLAIN_NUMBER = Pattern.compile("\\d+(?:\\.\\d+)?");
    private static final Pattern SLASH_DATE = Pattern.compile("(\\d{1,2})[/.-](\\d{1,2})[/.-](\\d{4})");

    private final SchemaContract contract;

    ChangeRules(SchemaContract contract) {
        this.contract = contract;
    }

    Evaluation evaluate(ObjectNode original, ObjectNode candidate, String kafkaKey) {
        List<String> violations = new ArrayList<>(contract.violations(candidate));
        List<String> flags = new ArrayList<>();
        ContractPolicy policy = contract.policy();

        for (String f : policy.identityFields()) {
            JsonNode before = original.get(f);
            if (before == null || before.isNull() || !before.equals(candidate.get(f))) {
                violations.add(f + " must be present in the original and unchanged");
            }
        }
        String keyField = policy.keyField();
        if (kafkaKey != null && keyField != null && !kafkaKey.equals(textOrNull(candidate.get(keyField)))) {
            violations.add(keyField + " must equal the Kafka key '" + kafkaKey + "'");
        }

        fieldSetChanges(original, candidate, flags);
        for (FieldSpec spec : contract.fields().values()) {
            JsonNode before = original.get(spec.name());
            JsonNode after = candidate.get(spec.name());
            if (before == null || after == null) {
                continue;
            }
            if (spec.isNumeric()) {
                numberChange(spec, before, after, violations, flags);
            } else if (spec.isDateTime()) {
                dateChange(spec, before, after, flags);
            } else if (spec.kind() == FieldSpec.Kind.STRING && !policy.identityFields().contains(spec.name())) {
                stringChange(spec, before, after, flags);
            }
        }
        return new Evaluation(violations, flags);
    }

    private static void fieldSetChanges(ObjectNode original, ObjectNode candidate, List<String> flags) {
        Set<String> added = new LinkedHashSet<>();
        candidate.fieldNames().forEachRemaining(f -> {
            if (!original.has(f)) added.add(f);
        });
        Set<String> dropped = new LinkedHashSet<>();
        original.fieldNames().forEachRemaining(f -> {
            if (!candidate.has(f)) dropped.add(f);
        });
        if (!added.isEmpty()) {
            if (dropped.size() >= added.size()) {
                flags.add("fields renamed " + dropped + " -> " + added + "; the mapping cannot be verified");
            } else {
                flags.add("absent in the original and filled in by the agent: " + added);
            }
        }
    }

    /** A repair may change how a number is written ("19.99 USD" to 19.99), never its value. */
    private static void numberChange(FieldSpec spec, JsonNode before, JsonNode after,
                                     List<String> violations, List<String> flags) {
        if (!after.isNumber()) {
            return; // the shape check already rejects it
        }
        BigDecimal was = numericValue(before);
        if (was == null) {
            flags.add("original " + spec.name() + " " + before + " has no unambiguous numeric value to compare with");
        } else if (was.compareTo(after.decimalValue()) != 0) {
            violations.add(spec.name() + " value changed from " + was.toPlainString() + " to "
                    + after.decimalValue().toPlainString());
        }
    }

    private static void dateChange(FieldSpec spec, JsonNode before, JsonNode after, List<String> flags) {
        String was = textOrNull(before);
        String now = textOrNull(after);
        if (was == null) {
            return;
        }
        Matcher m = SLASH_DATE.matcher(was.trim());
        if (m.matches()) {
            int a = Integer.parseInt(m.group(1));
            int b = Integer.parseInt(m.group(2));
            if (a <= 12 && b <= 12 && a != b) {
                flags.add(spec.name() + " '" + was + "' is ambiguous (day/month order unknown)");
            }
        }
        if (now != null && !was.equals(now) && parseTimestamp(was) == null) {
            flags.add(spec.name() + " reformatted from '" + was + "'; time of day and zone were assumed");
        }
    }

    /** A different spelling of the same value (case, padding) is formatting; anything else is a mapping. */
    private static void stringChange(FieldSpec spec, JsonNode before, JsonNode after, List<String> flags) {
        String was = textOrNull(before);
        String now = textOrNull(after);
        if (was != null && now != null && !asciiEqualsIgnoreCase(was.trim(), now)) {
            flags.add(spec.name() + " mapped from '" + was + "' to '" + now + "'");
        }
    }

    /** Number in the original, or a plain number embedded in text such as "19.99 USD"; null if none or ambiguous. */
    private static BigDecimal numericValue(JsonNode value) {
        if (value.isNumber()) {
            return value.decimalValue();
        }
        if (!value.isTextual()) {
            return null;
        }
        Matcher m = PLAIN_NUMBER.matcher(value.asText());
        if (!m.find() || value.asText().contains(",")) {
            return null;
        }
        BigDecimal found = new BigDecimal(m.group());
        return m.find() ? null : found; // two numbers in the text: ambiguous
    }

    /**
     * Case-insensitive for ASCII letters only. {@code String.equalsIgnoreCase} applies Unicode case rules, under
     * which for example the long s ("ſhipped") equals "SHIPPED", so a real remapping would pass as mere formatting.
     */
    static boolean asciiEqualsIgnoreCase(String a, String b) {
        if (a.length() != b.length()) {
            return false;
        }
        for (int i = 0; i < a.length(); i++) {
            char x = a.charAt(i);
            char y = b.charAt(i);
            if (x != y && !(isAsciiLetter(x) && isAsciiLetter(y) && (x | 0x20) == (y | 0x20))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAsciiLetter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    private static String textOrNull(JsonNode n) {
        return n != null && n.isTextual() ? n.asText() : null;
    }

    private static OffsetDateTime parseTimestamp(String s) {
        if (s == null) {
            return null;
        }
        try {
            return OffsetDateTime.parse(s);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
