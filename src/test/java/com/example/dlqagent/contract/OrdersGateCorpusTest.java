package com.example.dlqagent.contract;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.dlqagent.domain.Evaluation;
import com.example.dlqagent.support.OrdersContract;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * A regression guard over a large corpus: every order field crossed with awkward values, and every old/new value pair
 * per field evaluated as a repair (about 58,000 evaluations). The digest below was recorded from the schema-driven gate
 * when it was verified, in a differential run, to agree with the hand-written order validator it replaced on all of
 * them except one deliberate difference (see OrdersContractTest). If a rule change alters any verdict or risk flag,
 * the digest changes and this test fails: look at what changed, then update the digest on purpose.
 */
class OrdersGateCorpusTest {

    private static final String EXPECTED_DIGEST = "a2fea003a38fbc9fb0874e13e38f4bf68adf9dafa15bae31adf185d8f2127b34";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<String> FIELDS = List.of("orderId", "customerId", "amount", "currency", "status", "createdAt");
    private static final String BASE = """
            {"orderId":"o-1","customerId":"c-1","amount":19.99,"currency":"USD","status":"PAID","createdAt":"2025-04-03T10:00:00Z"}""";
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

    private final SchemaDrivenTopicAdapter gate = OrdersContract.adapter();

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

    private String line(String key, ObjectNode original, ObjectNode candidate) {
        Evaluation e = gate.evaluate(original.toString(), candidate.toString(), key);
        return key + "|" + original + "|" + candidate + "|" + (e.rejected() ? "REJECT" : "ACCEPT") + "|"
                + new TreeSet<>(e.riskFlags());
    }

    private List<String> corpus() {
        List<String> lines = new ArrayList<>();
        for (String key : new String[] {null, "o-1", "other"}) {
            for (String f : FIELDS) {
                for (String before : VALUES) {
                    for (String after : VALUES) {
                        lines.add(line(key, with(f, before), with(f, after)));
                    }
                }
            }
        }
        for (String f : FIELDS) {
            ObjectNode renamed = base();
            renamed.set(f + "Old", renamed.remove(f));
            lines.add(line("o-1", renamed, base()));
            ObjectNode missing = base();
            missing.remove(f);
            lines.add(line("o-1", missing, base()));
            ObjectNode extra = base();
            extra.put("junk", 1);
            lines.add(line("o-1", extra, base()));
        }
        // standalone validation of every single-field variant
        for (String f : FIELDS) {
            for (String v : VALUES) {
                lines.add("validate|" + with(f, v) + "|" + (gate.validate(with(f, v).toString()).isEmpty() ? "VALID" : "INVALID"));
            }
        }
        return lines;
    }

    @Test
    void verdictsAndFlagsOverTheCorpusAreUnchanged() throws Exception {
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        List<String> lines = corpus();
        for (String l : lines) {
            sha.update(l.getBytes(StandardCharsets.UTF_8));
            sha.update((byte) '\n');
        }
        String digest = HexFormat.of().formatHex(sha.digest());

        assertThat(lines).hasSizeGreaterThan(58_000);
        assertThat(digest).as("corpus digest (update EXPECTED_DIGEST only if the change is intended)").isEqualTo(EXPECTED_DIGEST);
    }
}
