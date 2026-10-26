package com.example.dlqagent.orders;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class OrderContractValidatorTest {

    private final OrderContractValidator gate = new OrderContractValidator(Clock.fixed(Instant.parse("2025-06-01T00:00:00Z"), ZoneOffset.UTC));

    private static String order(String amount, String createdAt) {
        return """
                {"orderId":"o-1","customerId":"c-1","amount":%s,"currency":"USD","status":"PAID","createdAt":%s}"""
                .formatted(amount, createdAt);
    }

    private static final String VALID = order("19.99", "\"2025-04-03T10:00:00Z\"");

    private EvaluationResult evaluate(String original, String candidate) {
        return gate.evaluate(original, candidate, "o-1");
    }

    // ---- schema rules --------------------------------------------------------------------------------------

    @Test
    void validPayloadHasNoViolationsAndYieldsTypedEvent() {
        EvaluationResult r = evaluate(VALID, VALID);

        assertThat(r.passed()).isTrue();
        assertThat(r.event().amount()).isEqualByComparingTo("19.99");
        assertThat(r.event().status()).isEqualTo(OrderStatus.PAID);
        assertThat(r.event().createdAt()).isEqualTo(Instant.parse("2025-04-03T10:00:00Z"));
    }

    @Test
    void validateReportsEveryBrokenRule() {
        var violations = gate.validate("""
                {"orderId":"bad id!","customerId":"","amount":-5.123,"currency":"usd","status":"LOST",
                 "createdAt":"yesterday","extra":1}""");

        assertThat(violations).anyMatch(v -> v.contains("orderId"))
                .anyMatch(v -> v.contains("customerId"))
                .anyMatch(v -> v.contains("greater than 0"))
                .anyMatch(v -> v.contains("2 decimal places"))
                .anyMatch(v -> v.contains("currency"))
                .anyMatch(v -> v.contains("status"))
                .anyMatch(v -> v.contains("createdAt"))
                .anyMatch(v -> v.contains("unknown field 'extra'"));
    }

    @Test
    void amountMustBeANumberNotText() {
        assertThat(gate.validate(order("\"19.99\"", "\"2025-04-03T10:00:00Z\"")))
                .contains("amount must be a JSON number");
    }

    @Test
    void amountUpperBoundAndZero() {
        assertThat(gate.validate(order("10000000000", "\"2025-04-03T10:00:00Z\""))).anyMatch(v -> v.contains("below"));
        assertThat(gate.validate(order("0", "\"2025-04-03T10:00:00Z\""))).anyMatch(v -> v.contains("greater than 0"));
        assertThat(gate.validate(order("100", "\"2025-04-03T10:00:00Z\""))).isEmpty();
    }

    @Test
    void unknownCurrencyCodeIsRejected() {
        assertThat(gate.validate(VALID.replace("USD", "ZZZ"))).anyMatch(v -> v.contains("ISO-4217"));
    }

    @Test
    void futureTimestampBeyondSkewIsRejected() {
        assertThat(gate.validate(order("1", "\"2025-06-01T00:10:00Z\""))).contains("createdAt is in the future");
        assertThat(gate.validate(order("1", "\"2025-06-01T00:04:00Z\""))).isEmpty();
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

        assertThat(evaluate(original, VALID).passed()).isTrue();
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
        assertThat(gate.evaluate(VALID, VALID, null).passed()).isTrue();
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
            assertThat(result.passed()).isTrue();
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
        assertThat(result.passed()).isTrue();
        assertThat(result.riskFlags()).anyMatch(v -> v.contains("renamed") && v.contains("total") && v.contains("amount"));
    }

    @Test
    void unparseableOriginalCannotBeVerified() {
        assertThat(evaluate("not json", VALID).violations()).anyMatch(v -> v.contains("cannot be verified"));
    }

    @Test
    void unparseableCandidateIsRejected() {
        assertThat(evaluate(VALID, "{").passed()).isFalse();
    }

    @Test
    void passedResultCarriesNoViolationsAndFailedResultCarriesNoEvent() {
        assertThat(evaluate(VALID, VALID.replace("PAID", "LOST")).event()).isNull();
        assertThat(evaluate(VALID, VALID).violations()).isEmpty();
        assertThat(new BigDecimal("19.99")).isEqualByComparingTo(evaluate(VALID, VALID).event().amount());
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
        assertThat(evaluate(extraField, VALID).passed()).isTrue();
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

        assertThat(result.passed()).isFalse();
        assertThat(result.riskFlags()).isEmpty();
    }

    @Test
    void unicodeLookalikesAreAMappingNotACaseChange() {
        // "\u017Fhipped" starts with a long s: String.equalsIgnoreCase says it equals "SHIPPED", which would hide a
        // real remapping as formatting and let it through to Tier 1.
        String longS = VALID.replace("\"PAID\"", "\"\u017Fhipped\"");
        String shipped = VALID.replace("\"PAID\"", "\"SHIPPED\"");

        assertThat(evaluate(longS, shipped).riskFlags()).anyMatch(v -> v.contains("status mapped"));
        assertThat(OrderContractValidator.asciiEqualsIgnoreCase("shipped", "SHIPPED")).isTrue();
        assertThat(OrderContractValidator.asciiEqualsIgnoreCase("\u017Fhipped", "SHIPPED")).isFalse();
        assertThat(OrderContractValidator.asciiEqualsIgnoreCase("u\u017Fd", "USD")).isFalse();
        assertThat(OrderContractValidator.asciiEqualsIgnoreCase("USD", "USDX")).isFalse();
    }
}
