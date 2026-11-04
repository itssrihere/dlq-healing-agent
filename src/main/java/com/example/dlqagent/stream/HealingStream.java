package com.example.dlqagent.stream;

import com.example.dlqagent.domain.TopicAdapter;
import com.example.dlqagent.retry.ReplayCircuitBreaker;

/**
 * Everything the engine needs to heal one DLQ: where it reads, the only topic it may replay to, the contract that
 * judges repairs, the tier policy, the retry budget and the circuit breaker. Immutable except the breaker, which is
 * deliberately this stream's own, so one stream's failing downstream does not pause another's retries.
 */
public final class HealingStream {

    private final String name;
    private final String dlqTopic;
    private final String sourceTopic;
    private final String groupId;
    private final TopicAdapter adapter;
    private final boolean autoReplay;
    private final double autoReplayMinConfidence;
    private final int maxAttempts;
    private final ReplayCircuitBreaker breaker;

    /** Public so tests can build a stream around a mock breaker or adapter; the application builds them in the registry. */
    public HealingStream(String name, String dlqTopic, String sourceTopic, String groupId, TopicAdapter adapter,
                  boolean autoReplay, double autoReplayMinConfidence, int maxAttempts, ReplayCircuitBreaker breaker) {
        this.name = name;
        this.dlqTopic = dlqTopic;
        this.sourceTopic = sourceTopic;
        this.groupId = groupId;
        this.adapter = adapter;
        this.autoReplay = autoReplay;
        this.autoReplayMinConfidence = autoReplayMinConfidence;
        this.maxAttempts = maxAttempts;
        this.breaker = breaker;
    }

    public String name() {
        return name;
    }

    public String dlqTopic() {
        return dlqTopic;
    }

    /** The only topic this stream may replay to. The {@code original-topic} header on a failed event must equal it. */
    public String sourceTopic() {
        return sourceTopic;
    }

    public String groupId() {
        return groupId;
    }

    public TopicAdapter adapter() {
        return adapter;
    }

    /** False: every repair of this stream waits for a human, however confident the model is. */
    public boolean autoReplay() {
        return autoReplay;
    }

    public double autoReplayMinConfidence() {
        return autoReplayMinConfidence;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public ReplayCircuitBreaker breaker() {
        return breaker;
    }
}
