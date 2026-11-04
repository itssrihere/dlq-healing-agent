package com.example.dlqagent.infrastructure;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
        @Valid @NotNull Approval approval,
        @NotEmpty List<@Valid @NotNull StreamConfig> streams) {

    /** Cross-stream rules a per-field annotation cannot express. They stop the service at start-up. */
    public DlqAgentProperties {
        streams = streams == null ? null : List.copyOf(streams);
        if (streams != null) {
            Set<String> names = new HashSet<>();
            Set<String> dlqTopics = new HashSet<>();
            Set<String> sourceTopics = new HashSet<>();
            Set<String> groups = new HashSet<>();
            for (StreamConfig s : streams) {
                if (s == null) {
                    continue;
                }
                unique(names.add(s.name()), "stream name", s.name());
                unique(dlqTopics.add(s.dlqTopic()), "DLQ topic", s.dlqTopic());
                unique(sourceTopics.add(s.sourceTopic()), "source topic", s.sourceTopic());
                unique(groups.add(s.groupIdOrDefault()), "consumer group id", s.groupIdOrDefault());
            }
            // A DLQ that is also somebody's source topic would feed the healer its own output.
            for (StreamConfig s : streams) {
                if (s != null && (dlqTopics.contains(s.sourceTopic()) || sourceTopics.contains(s.dlqTopic()))) {
                    throw new IllegalArgumentException("dlqagent.streams: a topic cannot be both a DLQ and a source topic ('"
                            + s.name() + "')");
                }
            }
        }
    }

    private static void unique(boolean fresh, String what, String value) {
        if (!fresh) {
            throw new IllegalArgumentException("dlqagent.streams: duplicate " + what + " '" + value + "'");
        }
    }

    /** Topic housekeeping shared by every stream. */
    public record Topics(
            @Min(1) int partitions,
            boolean declare) {}

    /**
     * One DLQ and everything needed to heal it. A DLQ belongs to exactly one source topic; the {@code original-topic}
     * header on a failed event is untrusted and must name that topic, or the event is quarantined.
     *
     * @param groupId                 consumer group; defaults to {@code dlq-healing-agent-<name>}. Changing the group id
     *                                of a stream that already has history makes it re-read its DLQ from the start
     * @param autoReplay              false: every repair of this stream waits for a human (Tier 2), never Tier 1
     * @param autoReplayMinConfidence overrides {@code gate.auto-replay-min-confidence} for this stream
     * @param maxAttempts             overrides {@code retry.max-attempts} for this stream
     */
    public record StreamConfig(
            @NotBlank @Pattern(regexp = "[a-z0-9][a-z0-9-]{0,62}") String name,
            @NotBlank String dlqTopic,
            @NotBlank String sourceTopic,
            String groupId,
            @Valid @NotNull Contract contract,
            Boolean autoReplay,
            @DecimalMin("0.0") @DecimalMax("1.0") Double autoReplayMinConfidence,
            @Min(1) Integer maxAttempts) {

        public StreamConfig {
            if (dlqTopic != null && dlqTopic.equals(sourceTopic)) {
                throw new IllegalArgumentException("dlqagent.streams['" + name + "']: dlq-topic and source-topic must differ");
            }
        }

        public String groupIdOrDefault() {
            return groupId == null || groupId.isBlank() ? "dlq-healing-agent-" + name : groupId;
        }

        public boolean autoReplayEnabled() {
            return autoReplay == null || autoReplay;
        }
    }

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

    /**
     * A stream's contract. {@code schema} is the JSON Schema (the single source of truth for shape). The rest is
     * the small policy a schema cannot express and a model must never set for itself: which fields are identity (never
     * edited), which field must equal the Kafka key, and which timestamps may not be in the future. {@code tables}
     * are the database tables the model may describe with its read-only tool for this stream.
     */
    public record Contract(
            @NotBlank String schema,
            @NotEmpty List<String> identityFields,
            String keyField,
            List<String> noFutureFields,
            List<String> tables) {

        public Contract {
            identityFields = identityFields == null ? List.of() : List.copyOf(identityFields);
            noFutureFields = noFutureFields == null ? List.of() : List.copyOf(noFutureFields);
            tables = tables == null ? List.of() : List.copyOf(tables);
        }
    }

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
