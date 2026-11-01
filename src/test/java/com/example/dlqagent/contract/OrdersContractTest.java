package com.example.dlqagent.contract;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.dlqagent.domain.Evaluation;
import com.example.dlqagent.support.OrdersContract;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The order contract (contracts/orders.schema.json plus the orders policy) as seen through the generic gate. */
class OrdersContractTest {

    private final SchemaDrivenTopicAdapter gate = OrdersContract.adapter();

    private static String order(String amount, String createdAt) {
        return """
                {"orderId":"o-1","customerId":"c-1","amount":%s,"currency":"USD","status":"PAID","createdAt":%s}"""
                .formatted(amount, createdAt);
    }

    private static final String VALID = order("19.99", "\"2025-04-03T10:00:00Z\"");

    private Evaluation evaluate(String original, String candidate) {
        return gate.evaluate(original, candidate, "o-1");
    }

    // ---- shape rules ---------------------------------------------------------------------------------------

    @Test
    void validPayloadHasNoViolations() {
        assertThat(evaluate(VALID, VALID).rejected()).isFalse();
        assertThat(evaluate(VALID, VALID).riskFlags()).isEmpty();
    }

    @Test
    void validateReportsEveryBrokenRuleAndNamesTheField() {
        var violations = gate.validate("""
                {"orderId":"bad id!","customerId":"","amount":-5.123,"currency":"usd","status":"LOST",
                 "createdAt":"yesterday","extra":1}""");

        assertThat(violations).anyMatch(v -> v.startsWith("orderId "))
                .anyMatch(v -> v.startsWith("customerId "))
                .anyMatch(v -> v.startsWith("amount ") && v.contains("exclusive minimum"))
                .anyMatch(v -> v.startsWith("amount ") && v.contains("multiple of 0.01"))
                .anyMatch(v -> v.startsWith("currency "))
                .anyMatch(v -> v.startsWith("status "))
                .anyMatch(v -> v.startsWith("createdAt "))
                .anyMatch(v -> v.contains("'extra'"));
    }

    @Test
    void missingRequiredFieldIsNamed() {
        assertThat(gate.validate(VALID.replace(",\"createdAt\":\"2025-04-03T10:00:00Z\"", "")))
                .anyMatch(v -> v.contains("required") && v.contains("createdAt"));
    }

    @Test
    void amountMustBeANumberNotText() {
        assertThat(gate.validate(order("\"19.99\"", "\"2025-04-03T10:00:00Z\"")))
                .anyMatch(v -> v.startsWith("amount ") && v.contains("number expected"));
    }

    @Test
    void amountUpperBoundAndZero() {
        assertThat(gate.validate(order("10000000000", "\"2025-04-03T10:00:00Z\""))).anyMatch(v -> v.contains("exclusive maximum"));
        assertThat(gate.validate(order("0", "\"2025-04-03T10:00:00Z\""))).anyMatch(v -> v.contains("exclusive minimum"));
        assertThat(gate.validate(order("100", "\"2025-04-03T10:00:00Z\""))).isEmpty();
    }

    /**
     * Deliberate difference from the hand-written validator this replaced: it also demanded a real ISO-4217 code.
     * The consumer's own contract (the orders table CHECK) requires only three uppercase letters, so that is the
     * contract. Inventing a different currency is still caught, as a mapping a human must confirm.
     */
    @Test
    void anyThreeUppercaseLettersIsAValidCurrencyButChangingOneIsFlagged() {
        assertThat(gate.validate(VALID.replace("USD", "ZZZ"))).isEmpty();
        assertThat(evaluate(VALID, VALID.replace("USD", "EUR")).riskFlags()).anyMatch(v -> v.contains("currency mapped"));
    }

    @Test
    void futureTimestampBeyondSkewIsRejectedAndNearFutureIsNot() {
        assertThat(gate.validate(order("1", "\"2025-06-01T00:10:00Z\""))).contains("createdAt is in the future");
        assertThat(gate.validate(order("1", "\"2025-06-01T00:04:00Z\""))).isEmpty();
    }

    @Test
    void timestampsAreParsedTheWayTheJavaConsumerParsesThem() {
        assertThat(gate.validate(order("1", "\"2025-04-03t10:00:00z\""))).isEmpty();      // lowercase t and z
        assertThat(gate.validate(order("1", "\"2025-04-03T10:00:00-00:00\""))).isEmpty(); // unknown-offset form
        assertThat(gate.validate(order("1", "\"2025-04-03T10:00:00\""))).isNotEmpty();    // no offset
        assertThat(gate.validate(order("1", "\"2025-02-30T10:00:00Z\""))).isNotEmpty();   // no such day
    }

    @Test
    void controlCharactersInAStringAreRejected() {
        // a regex "$" can match before a trailing newline, so the gate refuses control characters outright
        assertThat(gate.validate(VALID.replace("\"o-1\"", "\"o-1\\n\""))).anyMatch(v -> v.contains("control characters"));
    }

    @Test
    void nonObjectAndMalformedJsonAreRejected() {
        assertThat(gate.validate("[1,2]")).isNotEmpty();
        assertThat(gate.validate("{\"orderId\": ")).isNotEmpty();
        assertThat(gate.validate(VALID + " trailing")).isNotEmpty();
    }

    @Test
    void duplicateKeysAreRejected() {
        assertThat(gate.validate(VALID.replace("\"status\":\"PAID\"", "\"status\":\"PAID\",\"status\":\"NEW\"")))
                .isNotEmpty();
    }

    // ---- preservation rules ---------------------------------------------------------------------------------

    @Test
    void textAmountWithUnitIsRepairableToTheSameNumber() {
        String original = order("\"19.99 USD\"", "\"2025-04-03T10:00:00Z\"");

        assertThat(evaluate(original, VALID).rejected()).isFalse();
    }

    @Test
    void changedAmountValueIsRejected() {
        String original = order("\"19.99 USD\"", "\"2025-04-03T10:00:00Z\"");
        String hallucinated = order("29.99", "\"2025-04-03T10:00:00Z\"");

        assertThat(evaluate(original, hallucinated).violations()).anyMatch(v -> v.contains("amount value changed"));
    }

    @Test
    void ambiguousOrMultiNumberAmountCannotBeVerified() {
        String twoNumbers = order("\"19.99 or 24.99\"", "\"2025-04-03T10:00:00Z\"");
        String thousands = order("\"1,299.50\"", "\"2025-04-03T10:00:00Z\"");

        // Valid but unverifiable: a human decides (risk flag), it is not a hard failure.
        assertThat(evaluate(twoNumbers, VALID).violations()).isEmpty();
        assertThat(evaluate(twoNumbers, VALID).riskFlags()).anyMatch(v -> v.contains("no unambiguous numeric"));
        assertThat(evaluate(thousands, VALID).riskFlags()).anyMatch(v -> v.contains("no unambiguous numeric"));
    }

    @Test
    void identityFieldsMustBeUnchanged() {
        String candidate = VALID.replace("\"c-1\"", "\"c-2\"");

        assertThat(evaluate(VALID, candidate).violations()).anyMatch(v -> v.startsWith("customerId"));
    }

    @Test
    void orderIdMustMatchKafkaKey() {
        assertThat(gate.evaluate(VALID, VALID, "some-other-key").violations())
                .anyMatch(v -> v.contains("Kafka key"));
        assertThat(gate.evaluate(VALID, VALID, null).rejected()).isFalse();
    }

    @Test
    void ambiguousSlashDateIsFlaggedEvenIfCandidateLooksValid() {
        String original = order("19.99", "\"03/04/2025\"");

        var result = evaluate(original, VALID);
        assertThat(result.violations()).isEmpty();
        assertThat(result.riskFlags()).anyMatch(v -> v.contains("ambiguous"));
    }

    @Test
    void unambiguousSlashDatesAreAllowed() {
        // 25 cannot be a month; day == month. Not ambiguous, though the reformat still assumes time and zone.
        for (String date : List.of("25/04/2025", "03/03/2025")) {
            var result = evaluate(order("19.99", "\"" + date + "\""), VALID);
            assertThat(result.rejected()).isFalse();
            assertThat(result.riskFlags()).noneMatch(v -> v.contains("ambiguous"))
                    .anyMatch(v -> v.contains("time of day and zone were assumed"));
        }
    }

    @Test
    void fieldFilledInByTheAgentIsFlaggedNotRejected() {
        String original = VALID.replace("\"currency\":\"USD\",", "");

        var result = evaluate(original, VALID);
        assertThat(result.violations()).isEmpty();
        assertThat(result.riskFlags()).anyMatch(v -> v.contains("filled in by the agent") && v.contains("currency"));
    }

    @Test
    void renamedFieldIsAllowedButFlaggedBecauseTheMappingIsUnverifiable() {
        String original = VALID.replace("\"amount\"", "\"total\"");

        var result = evaluate(original, VALID);
        assertThat(result.rejected()).isFalse();
        assertThat(result.riskFlags()).anyMatch(v -> v.contains("renamed") && v.contains("total") && v.contains("amount"));
    }

    @Test
    void unparseableOriginalCannotBeVerified() {
        assertThat(evaluate("not json", VALID).violations()).anyMatch(v -> v.contains("cannot be verified"));
    }

    @Test
    void unparseableCandidateIsRejected() {
        assertThat(evaluate(VALID, "{").rejected()).isTrue();
    }

    // ---- Tier 1 versus Tier 2 classification -------------------------------------------------------------------

    @Test
    void mechanicalFormatFixesCarryNoRiskFlags() {
        String textAmount = order("\"19.99 USD\"", "\"2025-04-03T10:00:00Z\"");
        String lowercase = VALID.replace("\"PAID\"", "\"paid\"").replace("\"USD\"", "\"usd\"");
        String extraField = VALID.replace("\"status\"", "\"debug\":1,\"status\"");

        assertThat(evaluate(textAmount, VALID).riskFlags()).isEmpty();
        assertThat(evaluate(lowercase, VALID).riskFlags()).isEmpty();  // case normalization is formatting
        assertThat(evaluate(extraField, VALID).riskFlags()).isEmpty(); // dropping an unknown field
        assertThat(evaluate(extraField, VALID).rejected()).isFalse();
    }

    @Test
    void statusAndCurrencyRemappingIsFlagged() {
        String dispatched = VALID.replace("\"PAID\"", "\"dispatched\"");
        String dollar = VALID.replace("\"USD\"", "\"$\"");

        assertThat(evaluate(dispatched, VALID).riskFlags()).anyMatch(v -> v.contains("status mapped from 'dispatched'"));
        assertThat(evaluate(dollar, VALID).riskFlags()).anyMatch(v -> v.contains("currency mapped from '$'"));
    }

    @Test
    void numericallyChangedAmountIsAHardViolationNotAFlag() {
        var result = evaluate(VALID, order("29.99", "\"2025-04-03T10:00:00Z\""));

        assertThat(result.rejected()).isTrue();
        assertThat(result.riskFlags()).isEmpty();
    }

    @Test
    void unicodeLookalikesAreAMappingNotACaseChange() {
        // "ſhipped" starts with a long s: String.equalsIgnoreCase says it equals "SHIPPED", which would hide a
        // real remapping as formatting and let it through to Tier 1.
        String longS = VALID.replace("\"PAID\"", "\"ſhipped\"");
        String shipped = VALID.replace("\"PAID\"", "\"SHIPPED\"");

        assertThat(evaluate(longS, shipped).riskFlags()).anyMatch(v -> v.contains("status mapped"));
        assertThat(ChangeRules.asciiEqualsIgnoreCase("shipped", "SHIPPED")).isTrue();
        assertThat(ChangeRules.asciiEqualsIgnoreCase("ſhipped", "SHIPPED")).isFalse();
        assertThat(ChangeRules.asciiEqualsIgnoreCase("uſd", "USD")).isFalse();
        assertThat(ChangeRules.asciiEqualsIgnoreCase("USD", "USDX")).isFalse();
    }
}
