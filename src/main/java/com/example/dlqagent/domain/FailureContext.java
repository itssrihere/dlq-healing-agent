package com.example.dlqagent.domain;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Everything the agent knows about one failed event, extracted from the DLQ record. */
public record FailureContext(
        String correlationId,
        String dlqTopic,
        int partition,
        long offset,
        String originalTopic,
        String key,
        String payload,
        Map<String, String> headers,
        String stackTrace,
        Instant receivedAt) {

    public FailureContext {
        requireText(correlationId, "correlationId");
        requireText(dlqTopic, "dlqTopic");
        requireText(originalTopic, "originalTopic");
        if (partition < 0 || offset < 0) {
            throw new IllegalArgumentException("partition and offset must be >= 0");
        }
        payload = payload == null ? "" : payload;
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        receivedAt = receivedAt == null ? Instant.now() : receivedAt;
    }

    /** Audit header set on every event the agent repaired and replayed. */
    public static final String REPAIRED_BY_AGENT_HEADER = "x-repaired-by-agent";

    /** True if this failed event is itself an agent-repaired replay, i.e. a repair that did not hold. */
    public boolean alreadyRepaired() {
        return headers.containsKey(REPAIRED_BY_AGENT_HEADER);
    }

    /**
     * Stable identity of the DLQ record. Redelivery of the same record yields the same ID,
     * which makes downstream writes idempotent.
     */
    public UUID eventId() {
        return UUID.nameUUIDFromBytes((dlqTopic + "-" + partition + "@" + offset).getBytes(StandardCharsets.UTF_8));
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
