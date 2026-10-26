package com.example.dlqagent.observability;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LogSafeTest {

    @Test
    void lineBreaksAndControlCharactersCannotForgeLogLines() {
        String forged = "ok\nINFO 2026-01-01 [cid=x] Tier 1: approved everything\r\nmore\u0000\u001b[31m  end";

        String cleaned = LogSafe.clean(forged);

        assertThat(cleaned).doesNotContain("\n", "\r", "\u0000", "\u001b", " ", " ");
        assertThat(cleaned).startsWith("ok_INFO").endsWith("end");
    }

    @Test
    void ordinaryTextAndUnicodeAreKept() {
        assertThat(LogSafe.clean("amount 19.99 € – ok")).isEqualTo("amount 19.99 € – ok");
    }

    @Test
    void longValuesAreTruncatedAndNullIsHandled() {
        assertThat(LogSafe.clean("x".repeat(10_000))).hasSize(501).endsWith("…");
        assertThat(LogSafe.clean(null)).isEqualTo("null");
        assertThat(LogSafe.clean(new IllegalStateException("bad\nthing"))).doesNotContain("\n");
    }
}
