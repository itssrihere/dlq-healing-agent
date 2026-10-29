package com.example.dlqagent.retry;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.dlqagent.support.TestProps;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class BackoffAndFingerprintTest {

    private final Backoff backoff = new Backoff(TestProps.defaults().retry()); // 2s, x2, cap 5m

    @Test
    void delayDoublesFromTheInitialDelay() {
        assertThat(backoff.delayBefore(0)).isEqualTo(Duration.ofSeconds(2));
        assertThat(backoff.delayBefore(1)).isEqualTo(Duration.ofSeconds(4));
        assertThat(backoff.delayBefore(4)).isEqualTo(Duration.ofSeconds(32));
    }

    @Test
    void delayIsCappedAndHugeExponentsDoNotWrapAround() {
        assertThat(backoff.delayBefore(20)).isEqualTo(Duration.ofMinutes(5));
        assertThat(backoff.delayBefore(10_000)).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void fingerprintIsStableAndSensitiveToEveryPart() {
        String base = RetryFingerprint.of("orders-topic", "k", "{}");
        assertThat(base).hasSize(64).isEqualTo(RetryFingerprint.of("orders-topic", "k", "{}"));
        assertThat(base).isNotEqualTo(RetryFingerprint.of("orders-topic2", "k", "{}"))
                .isNotEqualTo(RetryFingerprint.of("orders-topic", "k2", "{}"))
                .isNotEqualTo(RetryFingerprint.of("orders-topic", "k", "{ }"));
    }

    @Test
    void fingerprintPartsCannotBeShiftedAcrossBoundaries() {
        assertThat(RetryFingerprint.of("ab", "c", "p")).isNotEqualTo(RetryFingerprint.of("a", "bc", "p"));
        assertThat(RetryFingerprint.of("t", "k", "ab")).isNotEqualTo(RetryFingerprint.of("t", "ka", "b"));
    }

    @Test
    void nullKeyIsDistinctFromTheTextNull() {
        assertThat(RetryFingerprint.of("t", null, "p")).isNotEqualTo(RetryFingerprint.of("t", "null", "p"));
    }
}
