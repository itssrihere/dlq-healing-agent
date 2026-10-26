package com.example.dlqagent.orders;

import com.example.dlqagent.json.StrictJson;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Currency;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The order topic's deterministic rules for repaired payloads. Plain Java, no model involved.
 * A candidate may be replayed only if {@link #evaluate} returns no violations.
 *
 * <p>Rules mirror the {@code orders} table (V3 migration), plus checks that the repair preserved meaning:
 * identity fields unchanged, Kafka key matches, amount value unchanged, no invented fields, no guessed dates.
 */
@Component
public class OrderContractValidator {

    public static final Set<String> FIELDS =
            Set.of("orderId", "customerId", "amount", "currency", "status", "createdAt");
    /** Identity fields: never editable, and must equal the original payload's values. */
    public static final Set<String> IMMUTABLE_FIELDS = Set.of("orderId", "customerId");

    private static final Pattern ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");
    private static final Pattern PLAIN_NUMBER = Pattern.compile("\\d+(?:\\.\\d+)?");
    private static final Pattern SLASH_DATE = Pattern.compile("(\\d{1,2})[/.-](\\d{1,2})[/.-](\\d{4})");
    private static final BigDecimal MAX_AMOUNT = new BigDecimal("1e10");
    private static final Duration CLOCK_SKEW = Duration.ofMinutes(5);

    private final Clock clock;

    @Autowired
    public OrderContractValidator() {
        this(Clock.systemUTC());
    }

    OrderContractValidator(Clock clock) {
        this.clock = clock;
    }

    /** Strict parse shared with the rest of the engine. @throws IllegalArgumentException if not a JSON object */
    public ObjectNode parseObject(String json) {
        return StrictJson.parseObject(json);
    }

    /** Schema and business-rule check on a standalone payload (also exposed to the LLM as a tool). */
    public List<String> validate(String json) {
        ObjectNode node;
        try {
            node = parseObject(json);
        } catch (IllegalArgumentException e) {
            return List.of("payload is " + e.getMessage());
        }
        return schemaViolations(node);
    }

    /** Full gate: schema rules plus preservation checks against the original payload. */
    public EvaluationResult evaluate(String originalPayload, String candidatePayload, String kafkaKey) {
        ObjectNode candidate;
        try {
            candidate = parseObject(candidatePayload);
        } catch (IllegalArgumentException e) {
            return new EvaluationResult(List.of("candidate is " + e.getMessage()), List.of(), null);
        }
        List<String> violations = new ArrayList<>(schemaViolations(candidate));
        List<String> flags = new ArrayList<>();

        ObjectNode original;
        try {
            original = parseObject(originalPayload);
        } catch (IllegalArgumentException e) {
            violations.add("original is " + e.getMessage() + "; identity fields cannot be verified");
            return new EvaluationResult(violations, flags, null);
        }
        preservation(original, candidate, kafkaKey, violations, flags);

        OrderEvent event = violations.isEmpty() ? toEvent(candidate) : null;
        return new EvaluationResult(violations, flags, event);
    }

    // ---- schema rules ------------------------------------------------------------------------------------------

    private List<String> schemaViolations(ObjectNode n) {
        List<String> v = new ArrayList<>();
        n.fieldNames().forEachRemaining(f -> {
            if (!FIELDS.contains(f)) {
                v.add("unknown field '" + f + "'");
            }
        });
        text(n, "orderId", v, s -> ID.matcher(s).matches(), "must match " + ID.pattern());
        text(n, "customerId", v, s -> ID.matcher(s).matches(), "must match " + ID.pattern());
        amount(n, v);
        text(n, "currency", v, OrderContractValidator::isKnownCurrency, "must be a valid ISO-4217 code");
        text(n, "status", v, OrderContractValidator::isStatus, "must be one of " + List.of(OrderStatus.values()));
        text(n, "createdAt", v, s -> parseTimestamp(s) != null, "must be an ISO-8601 timestamp with offset");
        JsonNode created = n.get("createdAt");
        if (created != null && created.isTextual()) {
            OffsetDateTime t = parseTimestamp(created.asText());
            if (t != null && t.toInstant().isAfter(clock.instant().plus(CLOCK_SKEW))) {
                v.add("createdAt is in the future");
            }
        }
        return v;
    }

    private static void text(ObjectNode n, String field, List<String> v,
                             java.util.function.Predicate<String> ok, String rule) {
        JsonNode node = n.get(field);
        if (node == null || node.isNull()) {
            v.add(field + " is required");
        } else if (!node.isTextual()) {
            v.add(field + " must be a string");
        } else if (!ok.test(node.asText())) {
            v.add(field + " " + rule);
        }
    }

    private static void amount(ObjectNode n, List<String> v) {
        JsonNode node = n.get("amount");
        if (node == null || node.isNull()) {
            v.add("amount is required");
        } else if (!node.isNumber()) {
            v.add("amount must be a JSON number");
        } else {
            BigDecimal a = node.decimalValue();
            if (a.signum() <= 0) {
                v.add("amount must be greater than 0");
            }
            if (a.stripTrailingZeros().scale() > 2) {
                v.add("amount must have at most 2 decimal places");
            }
            if (a.compareTo(MAX_AMOUNT) >= 0) {
                v.add("amount must be below " + MAX_AMOUNT.toPlainString());
            }
        }
    }

    // ---- preservation rules ------------------------------------------------------------------------------------

    /**
     * Hard violations: identity changed, key mismatch, amount value changed. These can never be replayed.
     * Risk flags: the candidate is valid but rests on an assumption a human should confirm (Tier 2).
     */
    private void preservation(ObjectNode original, ObjectNode candidate, String key,
                              List<String> violations, List<String> flags) {
        for (String f : IMMUTABLE_FIELDS) {
            String before = textOrNull(original.get(f));
            if (before == null || !before.equals(textOrNull(candidate.get(f)))) {
                violations.add(f + " must be present in the original and unchanged");
            }
        }
        if (key != null && !key.equals(textOrNull(candidate.get("orderId")))) {
            violations.add("orderId must equal the Kafka key '" + key + "'");
        }

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

        JsonNode originalAmount = original.get("amount");
        JsonNode candidateAmount = candidate.get("amount");
        if (originalAmount != null && candidateAmount != null && candidateAmount.isNumber()) {
            BigDecimal before = numericValue(originalAmount);
            if (before == null) {
                flags.add("original amount " + originalAmount + " has no unambiguous numeric value to compare with");
            } else if (before.compareTo(candidateAmount.decimalValue()) != 0) {
                violations.add("amount value changed from " + before.toPlainString() + " to "
                        + candidateAmount.decimalValue().toPlainString());
            }
        }

        // status and currency: a different spelling of the same value (case, padding) is formatting; anything else is a mapping.
        for (String field : List.of("status", "currency")) {
            String before = textOrNull(original.get(field));
            String after = textOrNull(candidate.get(field));
            if (before != null && after != null && !asciiEqualsIgnoreCase(before.trim(), after)) {
                flags.add(field + " mapped from '" + before + "' to '" + after + "'");
            }
        }

        String originalDate = textOrNull(original.get("createdAt"));
        String candidateDate = textOrNull(candidate.get("createdAt"));
        if (originalDate != null) {
            Matcher m = SLASH_DATE.matcher(originalDate.trim());
            if (m.matches()) {
                int a = Integer.parseInt(m.group(1));
                int b = Integer.parseInt(m.group(2));
                if (a <= 12 && b <= 12 && a != b) {
                    flags.add("createdAt '" + originalDate + "' is ambiguous (day/month order unknown)");
                }
            }
            if (candidateDate != null && !originalDate.equals(candidateDate) && parseTimestamp(originalDate) == null) {
                flags.add("createdAt reformatted from '" + originalDate + "'; time of day and zone were assumed");
            }
        }
    }

    /** Number in the original, or a plain number embedded in text such as "19.99 USD"; null if none or ambiguous. */
    private static BigDecimal numericValue(JsonNode amount) {
        if (amount.isNumber()) {
            return amount.decimalValue();
        }
        if (!amount.isTextual()) {
            return null;
        }
        Matcher m = PLAIN_NUMBER.matcher(amount.asText());
        if (!m.find() || amount.asText().contains(",")) {
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

    private static OrderEvent toEvent(ObjectNode n) {
        return new OrderEvent(
                n.get("orderId").asText(),
                n.get("customerId").asText(),
                n.get("amount").decimalValue(),
                n.get("currency").asText(),
                OrderStatus.valueOf(n.get("status").asText()),
                parseTimestamp(n.get("createdAt").asText()).toInstant());
    }

    private static String textOrNull(JsonNode n) {
        return n != null && n.isTextual() ? n.asText() : null;
    }

    private static boolean isKnownCurrency(String s) {
        if (!CURRENCY.matcher(s).matches()) {
            return false;
        }
        try {
            Currency.getInstance(s);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static boolean isStatus(String s) {
        try {
            OrderStatus.valueOf(s);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static OffsetDateTime parseTimestamp(String s) {
        try {
            return OffsetDateTime.parse(s);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
