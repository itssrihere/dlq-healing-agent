package com.example.dlqagent.infrastructure;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** Typed, validated application settings (prefix {@code dlqagent}). Fails fast on bad config. */
@Validated
@ConfigurationProperties(prefix = "dlqagent")
public record DlqAgentProperties(
        @Valid @NotNull Topics topics,
        @Valid @NotNull Retry retry,
        @Valid @NotNull Planner planner,
        @Valid @NotNull Gate gate,
        @Valid @NotNull Approval approval) {

    public record Topics(
            @NotBlank String main,
            @NotBlank String dlq,
            @Min(1) int partitions) {}

    /**
     * Strategy B. {@code maxAttempts} is the number of replays per event; the delay before replay n is
     * {@code initialDelay * multiplier^(n-1)}, capped at {@code maxDelay}. The poller wakes every
     * {@code pollInterval} and replays at most {@code batchSize} due events per wake-up.
     */
    public record Retry(
            @Min(1) int maxAttempts,
            @NotNull Duration initialDelay,
            @DecimalMin("1.0") double multiplier,
            @NotNull Duration maxDelay,
            @NotNull Duration pollInterval,
            @Min(1) int batchSize,
            @Valid @NotNull Breaker breaker) {

        public Retry {
            requirePositive(initialDelay, "initial-delay");
            requirePositive(maxDelay, "max-delay");
            requirePositive(pollInterval, "poll-interval");
            if (initialDelay != null && maxDelay != null && maxDelay.compareTo(initialDelay) < 0) {
                throw new IllegalArgumentException("dlqagent.retry.max-delay must not be shorter than initial-delay");
            }
        }

        private static void requirePositive(Duration d, String name) {
            if (d != null && (d.isZero() || d.isNegative())) {
                throw new IllegalArgumentException("dlqagent.retry." + name + " must be positive");
            }
        }
    }

    /**
     * Circuit breaker over replays. It opens after {@code failureThreshold} retried events came back to the DLQ
     * within {@code failureWindow}, stops all replays for {@code cooldown}, then lets one canary through and closes
     * if nothing comes back within {@code canaryWindow}. The canary window must be longer than the main
     * service's own retry time, or a failing canary is mistaken for a success.
     */
    public record Breaker(
            @Min(1) int failureThreshold,
            @NotNull Duration failureWindow,
            @NotNull Duration cooldown,
            @NotNull Duration canaryWindow) {

        public Breaker {
            for (Duration d : new Duration[] {failureWindow, cooldown, canaryWindow}) {
                if (d != null && (d.isZero() || d.isNegative())) {
                    throw new IllegalArgumentException("dlqagent.retry.breaker durations must be positive");
                }
            }
        }
    }

    public record Planner(
            @DecimalMin("0.0") @DecimalMax("1.0") double minConfidence,
            @Min(100) int maxPayloadChars,
            @Min(100) int maxStackTraceChars) {}

    /** Tier-1 / Tier-2 policy. Confidence can only demote a repair to Tier 2, never promote one. */
    public record Gate(
            @DecimalMin("0.0") @DecimalMax("1.0") double autoReplayMinConfidence) {}

    /** The single reviewer account for the approval API. The password has no default on purpose. */
    public record Approval(
            @NotBlank String username,
            @NotBlank String password) {

        @Override
        public String toString() {
            return "Approval[username=" + username + ", password=****]";
        }
    }
}
