package com.example.dlqagent.support;

import static org.mockito.Mockito.mock;

import com.example.dlqagent.contract.ContractPolicy;
import com.example.dlqagent.contract.SchemaContract;
import com.example.dlqagent.contract.SchemaDrivenTopicAdapter;
import com.example.dlqagent.domain.SchemaInspector;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;
import org.springframework.core.io.ClassPathResource;

/** The order contract as the application configures it, for unit tests that do not boot Spring. */
public final class OrdersContract {

    /** Fixed, so "in the future" is the same on every run. */
    public static final Clock CLOCK = Clock.fixed(Instant.parse("2025-06-01T00:00:00Z"), ZoneOffset.UTC);

    private OrdersContract() {}

    public static String schema() {
        try {
            return new ClassPathResource("contracts/orders.schema.json").getContentAsString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static ContractPolicy policy() {
        return new ContractPolicy(Set.of("orderId", "customerId"), "orderId", Set.of("createdAt"));
    }

    public static SchemaDrivenTopicAdapter adapter() {
        return new SchemaDrivenTopicAdapter("orders-topic", new SchemaContract(schema(), policy(), CLOCK),
                mock(SchemaInspector.class));
    }
}
