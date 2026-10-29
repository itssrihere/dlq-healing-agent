package com.example.dlqagent.retry;

import com.example.dlqagent.infrastructure.DlqAgentProperties;
import java.time.Duration;

/** Exponential backoff: {@code initialDelay * multiplier^(replaysAlreadySent)}, capped at {@code maxDelay}. */
final class Backoff {

    private final Duration initial;
    private final double multiplier;
    private final Duration max;

    Backoff(DlqAgentProperties.Retry retry) {
        this.initial = retry.initialDelay();
        this.multiplier = retry.multiplier();
        this.max = retry.maxDelay();
    }

    /** @param replaysAlreadySent 0 for the first replay */
    Duration delayBefore(int replaysAlreadySent) {
        double millis = initial.toMillis() * Math.pow(multiplier, Math.max(0, replaysAlreadySent));
        // Compare as doubles first: a large exponent overflows long and must not wrap around to a short delay.
        return millis >= max.toMillis() ? max : Duration.ofMillis((long) millis);
    }
}
