package com.example.dlqagent.support;

import static org.mockito.Mockito.mock;

import com.example.dlqagent.domain.SchemaInspector;
import com.example.dlqagent.infrastructure.DlqAgentProperties;
import com.example.dlqagent.domain.TopicAdapter;
import com.example.dlqagent.retry.ReplayCircuitBreaker;
import com.example.dlqagent.stream.HealingStream;
import com.example.dlqagent.stream.StreamRegistry;

/** Real streams built from {@link TestProps} for unit tests that do not boot Spring. */
public final class TestStreams {

    private TestStreams() {}

    public static StreamRegistry registry() {
        return registry(TestProps.defaults());
    }

    public static StreamRegistry registry(DlqAgentProperties props) {
        return new StreamRegistry(props, location -> OrdersContract.schemaAt(location), mock(SchemaInspector.class),
                OrdersContract.CLOCK);
    }

    /** An orders stream around the given breaker, with the real order adapter. */
    public static HealingStream orders(ReplayCircuitBreaker breaker) {
        return new HealingStream("orders", "orders-dlq", "orders-topic", "dlq-healing-agent", OrdersContract.adapter(),
                true, 0.9, 5, breaker);
    }

    /** An orders stream around the given adapter (for tests that mock the adapter). */
    public static HealingStream ordersWith(TopicAdapter adapter) {
        return new HealingStream("orders", "orders-dlq", "orders-topic", "dlq-healing-agent", adapter, true, 0.9, 5,
                new ReplayCircuitBreaker(OrdersContract.CLOCK, TestProps.defaults().retry().breaker()));
    }

    /** The orders stream: {@code orders-dlq} to {@code orders-topic}. */
    public static HealingStream orders() {
        return registry().byName("orders").orElseThrow();
    }
}
