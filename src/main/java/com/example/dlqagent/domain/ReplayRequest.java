package com.example.dlqagent.domain;

import java.util.Map;

/**
 * Everything needed to publish a repaired event back to its topic.
 *
 * @param headers    the failed event's own headers; audit headers are added (and any forged ones stripped) on send
 * @param approvedBy the human reviewer, or null for a Tier 1 automatic replay
 * @param retryAttempt replay number of a Strategy B retry (payload unchanged), or 0 for a repaired event
 */
public record ReplayRequest(
        String topic,
        String key,
        String payload,
        Map<String, String> headers,
        String correlationId,
        String summary,
        String approvedBy,
        int retryAttempt) {

    public ReplayRequest {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    /** A repaired event (Tier 1 or an approved Tier 2 repair). */
    public ReplayRequest(String topic, String key, String payload, Map<String, String> headers,
                         String correlationId, String summary, String approvedBy) {
        this(topic, key, payload, headers, correlationId, summary, approvedBy, 0);
    }

    /** Strategy B: the original payload, unchanged, sent again. */
    public static ReplayRequest retry(RetryEntry entry, int replayNumber) {
        return new ReplayRequest(entry.targetTopic(), entry.key(), entry.payload(), entry.headers(),
                entry.correlationId(), null, null, replayNumber);
    }

    public boolean isRetry() {
        return retryAttempt > 0;
    }

    public static ReplayRequest automatic(FailureContext ctx, RepairResult.Repaired repaired) {
        return new ReplayRequest(ctx.originalTopic(), ctx.key(), repaired.payload(), ctx.headers(),
                ctx.correlationId(), repaired.summary(), null);
    }
}
