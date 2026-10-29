package com.example.dlqagent.support;

import com.example.dlqagent.infrastructure.DlqAgentProperties;
import java.time.Duration;

/** Property sets for unit tests that do not boot Spring. */
public final class TestProps {

    private TestProps() {}

    /** Mirrors application.yml: auto-replay needs confidence of at least 0.9. */
    public static DlqAgentProperties defaults() {
        return new DlqAgentProperties(
                new DlqAgentProperties.Topics("orders-topic", "orders-dlq", 3),
                new DlqAgentProperties.Retry(5, Duration.ofSeconds(2), 2.0, Duration.ofMinutes(5),
                        Duration.ofSeconds(5), 10,
                        new DlqAgentProperties.Breaker(5, Duration.ofMinutes(1), Duration.ofSeconds(30), Duration.ofMinutes(2))),
                new DlqAgentProperties.Planner(0.7, 4000, 2000),
                new DlqAgentProperties.Gate(0.9),
                new DlqAgentProperties.Approval("reviewer", "unit-test-password"));
    }

    public static DlqAgentProperties withPlanner(int maxPayloadChars, int maxStackTraceChars) {
        DlqAgentProperties d = defaults();
        return new DlqAgentProperties(d.topics(), d.retry(),
                new DlqAgentProperties.Planner(0.7, maxPayloadChars, maxStackTraceChars), d.gate(), d.approval());
    }
}
