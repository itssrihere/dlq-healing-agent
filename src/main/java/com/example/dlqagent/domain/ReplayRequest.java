package com.example.dlqagent.domain;

import java.util.Map;

/**
 * Everything needed to publish a repaired event back to its topic.
 *
 * @param headers    the failed event's own headers; audit headers are added (and any forged ones stripped) on send
 * @param approvedBy the human reviewer, or null for a Tier 1 automatic replay
 */
public record ReplayRequest(
        String topic,
        String key,
        String payload,
        Map<String, String> headers,
        String correlationId,
        String summary,
        String approvedBy) {

    public ReplayRequest {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    public static ReplayRequest automatic(FailureContext ctx, RepairResult.Repaired repaired) {
        return new ReplayRequest(ctx.originalTopic(), ctx.key(), repaired.payload(), ctx.headers(),
                ctx.correlationId(), repaired.summary(), null);
    }
}
