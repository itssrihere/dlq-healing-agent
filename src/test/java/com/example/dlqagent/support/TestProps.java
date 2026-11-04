package com.example.dlqagent.support;

import com.example.dlqagent.infrastructure.DlqAgentProperties;
import java.time.Duration;

/** Property sets for unit tests that do not boot Spring. */
public final class TestProps {

    private TestProps() {}

    /** Mirrors application.yml: auto-replay needs confidence of at least 0.9. */
    public static DlqAgentProperties defaults() {
        return new DlqAgentProperties(
                new DlqAgentProperties.Topics(3, false),
                new DlqAgentProperties.Retry(5, Duration.ofSeconds(2), 2.0, Duration.ofMinutes(5),
                        Duration.ofSeconds(5), 10,
                        new DlqAgentProperties.Breaker(5, Duration.ofMinutes(1), Duration.ofSeconds(30), Duration.ofMinutes(2))),
                new DlqAgentProperties.Planner(0.7, 4000, 2000),
                new DlqAgentProperties.Gate(0.9),
                new DlqAgentProperties.Approval("reviewer", "unit-test-password"),
                java.util.List.of(ordersStream()));
    }

    /** Mirrors the orders stream in application.yml. */
    public static DlqAgentProperties.StreamConfig ordersStream() {
        return new DlqAgentProperties.StreamConfig("orders", "orders-dlq", "orders-topic", "dlq-healing-agent",
                new DlqAgentProperties.Contract("classpath:contracts/orders.schema.json",
                        java.util.List.of("orderId", "customerId"), "orderId", java.util.List.of("createdAt"),
                        java.util.List.of("orders")),
                null, null, null);
    }

    public static DlqAgentProperties withPlanner(int maxPayloadChars, int maxStackTraceChars) {
        DlqAgentProperties d = defaults();
        return new DlqAgentProperties(d.topics(), d.retry(),
                new DlqAgentProperties.Planner(0.7, maxPayloadChars, maxStackTraceChars), d.gate(), d.approval(),
                d.streams());
    }
}
