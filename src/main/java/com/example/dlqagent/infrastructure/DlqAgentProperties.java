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

    public record Retry(
            @Min(1) int maxAttempts,
            @NotNull Duration initialDelay,
            @DecimalMin("1.0") double multiplier,
            @NotNull Duration maxDelay) {}

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
