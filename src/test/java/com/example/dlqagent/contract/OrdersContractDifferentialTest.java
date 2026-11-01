package com.example.dlqagent.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.example.dlqagent.domain.Evaluation;
import com.example.dlqagent.domain.SchemaInspector;
import com.example.dlqagent.orders.EvaluationResult;
import com.example.dlqagent.orders.OrderContractValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

/**
 * The schema-driven gate must give the same verdicts as the hand-written order validator it replaces. Every field is
 * crossed with awkward values, and every old/new value pair per field is evaluated as a repair. Disagreements are
 * collected, not asserted one by one, so a failure shows all of them.
 */
class OrdersContractDifferentialTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<String> FIELDS = List.of("orderId", "customerId", "amount", "currency", "status", "createdAt");
    private static final String BASE = """
            {"orderId":"o-1","customerId":"c-1","amount":19.99,"currency":"USD","status":"PAID","createdAt":"2025-04-03T10:00:00Z"}""";

    /** Awkward values, as JSON text. */
    private static final List<String> VALUES = List.of(
            "\"\"", "\" \"", "\"o-1\"", "\"o 1\"", "\"o\\n\"", "\"o\\n1\"", "\"o\\u0000\"", "\"" + "a".repeat(64) + "\"", "\"" + "a".repeat(65) + "\"",
            "\"ünï\"", "\"ſhipped\"", "\"SHIPPED\"", "\"shipped\"", "\" PAID \"", "\"paid\"", "\"PAID\"", "\"NEW\"", "\"CANCELLED\"", "\"REFUNDED\"",
            "\"USD\"", "\"usd\"", "\"US\"", "\"USDX\"", "\"EUR\"", "\"XXX\"", "\"ZZZ\"", "\"QQQ\"",
            "\"19.99 USD\"", "\"19.99\"", "\"USD 19.99\"", "\"1,999.00\"", "\"12 and 13\"",
            "\"03/04/2025\"", "\"13/04/2025\"", "\"03/03/2025\"", "\"2025-04-03\"", "\"2025-04-03T10:00:00\"",
            "\"2025-04-03T10:00:00Z\"", "\"2025-04-03T10:00:00+02:00\"", "\"2025-04-03T10:00:00.123456789Z\"",
            "\"2025-04-03t10:00:00z\"", "\"2025-04-03T10:00:00+0200\"", "\"2025-02-30T10:00:00Z\"", "\"2025-04-03T23:59:60Z\"",
            "\"2999-01-01T00:00:00Z\"", "\"2025-04-03T10:00:00-00:00\"",
            "0", "-1", "19.99", "19.995", "20", "20.0", "1e10", "9999999999.99", "0.01", "0.001", "1E+2", "true", "null", "[1]", "{}");

    private final OrderContractValidator old = new OrderContractValidator();
    private final SchemaDrivenTopicAdapter adapter;

    OrdersContractDifferentialTest() throws IOException {
        String schema = new ClassPathResource("contracts/orders.schema.json").getContentAsString(StandardCharsets.UTF_8);
        adapter = new SchemaDrivenTopicAdapter("orders-topic",
                new SchemaContract(schema, new ContractPolicy(Set.of("orderId", "customerId"), "orderId", Set.of("createdAt")),
                        Clock.systemUTC()),
                mock(SchemaInspector.class));
    }

    private static ObjectNode base() {
        try {
            return (ObjectNode) JSON.readTree(BASE);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static ObjectNode with(String field, String valueJson) {
        try {
            ObjectNode n = base();
            n.set(field, JSON.readTree(valueJson));
            return n;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private List<ObjectNode> payloads() {
        List<ObjectNode> all = new ArrayList<>();
        all.add(base());
        for (String f : FIELDS) {
            ObjectNode missing = base();
            missing.remove(f);
            all.add(missing);
            ObjectNode renamed = base();
            renamed.set(f + "2", renamed.remove(f));
            all.add(renamed);
            for (String v : VALUES) {
                all.add(with(f, v));
            }
        }
        ObjectNode extra = base();
        extra.put("extra", "x");
        all.add(extra);
        all.add(JSON.createObjectNode());
        return all;
    }

    private static String describe(JsonNode n) {
        return n.toString();
    }

    /**
     * The one deliberate difference: the old validator also required a real ISO-4217 code, which a schema cannot
     * express. The consumer's own contract, the orders table CHECK, only requires three uppercase letters, and a
     * different currency is a mapping the gate flags for a human anyway.
     */
    private static boolean onlyBecauseNotARealCurrency(List<String> oldViolations, JsonNode candidate) {
        String c = candidate.path("currency").asText("");
        return !oldViolations.isEmpty() && c.matches("[A-Z]{3}")
                && oldViolations.stream().allMatch(v -> v.startsWith("currency must be a valid ISO-4217"));
    }

    private static void assertNone(List<String> disagreements, String what) {
        String sample = String.join("\n", disagreements.subList(0, Math.min(12, disagreements.size())));
        assertThat(disagreements).as("%s: %d unexplained disagreements, first ones:\n%s", what, disagreements.size(), sample).isEmpty();
    }

    @Test
    void standaloneValidationAgrees() {
        List<String> disagreements = new ArrayList<>();
        for (ObjectNode p : payloads()) {
            String json = p.toString();
            boolean oldRejects = !old.validate(json).isEmpty();
            boolean newRejects = !adapter.validate(json).isEmpty();
            if (oldRejects != newRejects && !(oldRejects && onlyBecauseNotARealCurrency(old.validate(json), p))) {
                disagreements.add((oldRejects ? "old rejects, new accepts: " : "old accepts, new rejects: ") + json
                        + "  old=" + old.validate(json) + " new=" + adapter.validate(json));
            }
        }
        assertNone(disagreements, "validate()");
    }

    @Test
    void evaluationOfRepairsAgrees() {
        List<String> disagreements = new ArrayList<>();
        for (String key : new String[] {null, "o-1", "other"}) {
            for (String f : FIELDS) {
                for (String before : VALUES) {
                    ObjectNode original = with(f, before);
                    for (String after : VALUES) {
                        ObjectNode candidate = with(f, after);
                        compare(original, candidate, key, disagreements);
                    }
                }
            }
        }
        // structural repairs: a dropped field, a renamed field, a filled-in field
        for (String f : FIELDS) {
            ObjectNode renamed = base();
            renamed.set(f + "Old", renamed.remove(f));
            compare(renamed, base(), "o-1", disagreements);
            ObjectNode missing = base();
            missing.remove(f);
            compare(missing, base(), "o-1", disagreements);
            ObjectNode extra = base();
            extra.put("junk", 1);
            compare(extra, base(), "o-1", disagreements);
        }
        assertNone(disagreements, "evaluate()");
    }

    private void compare(ObjectNode original, ObjectNode candidate, String key, List<String> out) {
        EvaluationResult o = old.evaluate(original.toString(), candidate.toString(), key);
        Evaluation n = adapter.evaluate(original.toString(), candidate.toString(), key);
        Set<String> oldFlags = new TreeSet<>(o.riskFlags());
        Set<String> newFlags = new TreeSet<>(n.riskFlags());
        boolean oldRejected = !o.violations().isEmpty();
        if (oldRejected && !n.rejected() && onlyBecauseNotARealCurrency(o.violations(), candidate)) {
            return;
        }
        if (oldRejected != n.rejected()) {
            out.add("VERDICT " + (oldRejected ? "old rejects, new accepts" : "old accepts, new rejects") + " key=" + key
                    + " orig=" + describe(original) + " cand=" + describe(candidate)
                    + " oldViolations=" + o.violations() + " newViolations=" + n.violations());
        } else if (!oldRejected && !oldFlags.equals(newFlags)) {
            out.add("FLAGS key=" + key + " orig=" + describe(original) + " cand=" + describe(candidate)
                    + " old=" + oldFlags + " new=" + newFlags);
        }
    }
}
