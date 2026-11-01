package com.example.dlqagent;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.dlqagent.contract.SchemaDrivenTopicAdapter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * "Keep the schema and the orders table in sync" as a failing test instead of a comment. The configured contract is
 * checked against the real table, which stands for the consumer's own storage:
 *
 * <ul>
 *   <li>Soundness, for every value tried: if the gate accepts a payload, the table must accept the row. Otherwise a
 *       "repaired" event would fail downstream again, which is the failure this service exists to prevent.
 *   <li>Equivalence for status and currency, where the schema should mirror the table's CHECK constraints exactly.
 * </ul>
 *
 * The gate is allowed to be stricter than the table (identifier characters, two decimal places, no future dates, a
 * strict timestamp); that only costs an extra human review, never a failed replay.
 */
class OrdersContractDriftIT extends AbstractIntegrationTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String BASE = """
            {"orderId":"o-1","customerId":"c-1","amount":19.99,"currency":"USD","status":"PAID","createdAt":"2025-04-03T10:00:00Z"}""";
    private static final List<String> FIELDS = List.of("orderId", "customerId", "amount", "currency", "status", "createdAt");
    private static final List<String> VALUES = List.of(
            "\"\"", "\" \"", "\"o-1\"", "\"o 1\"", "\"" + "a".repeat(64) + "\"", "\"" + "a".repeat(65) + "\"", "\"ünï\"",
            "\"SHIPPED\"", "\"shipped\"", "\" PAID \"", "\"paid\"", "\"PAID\"", "\"NEW\"", "\"CANCELLED\"", "\"REFUNDED\"",
            "\"USD\"", "\"usd\"", "\"US\"", "\"USDX\"", "\"EUR\"", "\"XXX\"", "\"ZZZ\"", "\"1A2\"",
            "\"2025-04-03T10:00:00Z\"", "\"2025-04-03T10:00:00+02:00\"", "\"2025-04-03T10:00:00.123456789Z\"",
            "\"2025-04-03t10:00:00z\"", "\"2025-04-03T10:00:00-00:00\"", "\"2025-04-03T10:00:00\"", "\"2025-04-03\"",
            "\"2025-02-30T10:00:00Z\"", "\"2025-04-03T23:59:60Z\"", "\"2025-04-03T10:00:00+0200\"", "\"2025-04-03T10:00:00.1234567Z\"",
            "0", "-1", "19.99", "19.995", "20", "1e10", "9999999999.99", "0.01", "0.001", "1E+2", "123456789012");

    @Autowired SchemaDrivenTopicAdapter gate;
    @Autowired JdbcClient jdbc;
    @Autowired TransactionTemplate tx;

    private static ObjectNode with(String field, String valueJson) throws Exception {
        ObjectNode n = (ObjectNode) JSON.readTree(BASE);
        n.set(field, JSON.readTree(valueJson));
        return n;
    }

    /** Would the table take this row? The insert is rolled back either way. */
    private boolean tableAccepts(ObjectNode order) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            status.setRollbackOnly();
            try {
                jdbc.sql("""
                        INSERT INTO orders (order_id, customer_id, amount, currency, status, created_at)
                        VALUES (:id, :customer, :amount, :currency, :status, CAST(:createdAt AS timestamptz))
                        """)
                        .param("id", order.get("orderId").asText())
                        .param("customer", order.get("customerId").asText())
                        .param("amount", order.get("amount").decimalValue())
                        .param("currency", order.get("currency").asText())
                        .param("status", order.get("status").asText())
                        .param("createdAt", order.get("createdAt").asText())
                        .update();
                return true;
            } catch (DataAccessException e) {
                return false;
            }
        }));
    }

    private static boolean wellTyped(ObjectNode order) {
        return order.get("amount").isNumber() && FIELDS.stream().filter(f -> !f.equals("amount"))
                .allMatch(f -> order.get(f).isTextual());
    }

    @Test
    void whateverTheGateAcceptsTheTableAccepts() throws Exception {
        List<String> unsound = new ArrayList<>();
        int accepted = 0;
        for (String f : FIELDS) {
            for (String v : VALUES) {
                ObjectNode order = with(f, v);
                if (gate.validate(order.toString()).isEmpty()) {
                    accepted++;
                    if (!tableAccepts(order)) {
                        unsound.add(order.toString());
                    }
                }
            }
        }
        assertThat(accepted).as("the corpus must include payloads the gate accepts").isGreaterThan(20);
        assertThat(unsound).as("payloads the gate accepts but the orders table rejects").isEmpty();
    }

    @Test
    void statusAndCurrencyRulesMatchTheTablesCheckConstraintsExactly() throws Exception {
        List<String> mismatches = new ArrayList<>();
        for (String f : List.of("status", "currency")) {
            for (String v : VALUES) {
                ObjectNode order = with(f, v);
                if (!wellTyped(order)) {
                    continue;
                }
                // judge only this field: other fields are the valid base, so any difference comes from f
                boolean gateAccepts = gate.validate(order.toString()).isEmpty();
                boolean tableAccepts = tableAccepts(order);
                if (gateAccepts != tableAccepts) {
                    mismatches.add(f + "=" + v + " gate=" + gateAccepts + " table=" + tableAccepts);
                }
            }
        }
        assertThat(mismatches).as("schema and table disagree").isEmpty();
    }

    @Test
    void theAmountBoundsMatchTheTable() throws Exception {
        // amount > 0 and NUMERIC(12,2): the gate must refuse exactly what the table's range or CHECK would refuse
        for (String v : List.of("0", "-1", "0.01", "9999999999.99", "1e10", "123456789012")) {
            ObjectNode order = with("amount", v);
            boolean gateAccepts = gate.validate(order.toString()).isEmpty();
            assertThat(gateAccepts).as("amount %s: gate vs table", v).isEqualTo(tableAccepts(order));
        }
    }

    @Test
    void rowsAreNeverActuallyWrittenByTheseChecks() {
        assertThat(jdbc.sql("SELECT count(*) FROM orders").query(Integer.class).single()).isZero();
    }
}
