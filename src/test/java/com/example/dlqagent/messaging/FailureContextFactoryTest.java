package com.example.dlqagent.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.support.TestProps;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class FailureContextFactoryTest {

    private final FailureContextFactory factory = new FailureContextFactory(TestProps.defaults());

    private static ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>("orders-dlq", 2, 41L, "order-1", value);
    }

    @Test
    void extractsCorrelationIdOriginalTopicAndStackTraceFromHeaders() {
        var rec = record("{\"id\":1}");
        rec.headers().add(DlqHeaders.CORRELATION_ID, "cid-123".getBytes(StandardCharsets.UTF_8));
        rec.headers().add(DlqHeaders.ORIGINAL_TOPIC, "orders-v2".getBytes(StandardCharsets.UTF_8));
        rec.headers().add(DlqHeaders.EXCEPTION_STACKTRACE, "java.lang.Boom".getBytes(StandardCharsets.UTF_8));
        rec.headers().add("tenant", "acme".getBytes(StandardCharsets.UTF_8));
        rec.headers().add("kafka_dlt-original-partition", new byte[] {0, 0, 0, 1});

        FailureContext ctx = factory.from(rec);

        assertThat(ctx.correlationId()).isEqualTo("cid-123");
        assertThat(ctx.originalTopic()).isEqualTo("orders-v2");
        assertThat(ctx.stackTrace()).isEqualTo("java.lang.Boom");
        assertThat(ctx.partition()).isEqualTo(2);
        assertThat(ctx.offset()).isEqualTo(41L);
        assertThat(ctx.headers()).containsEntry("tenant", "acme")
                .doesNotContainKey("kafka_dlt-original-partition")
                .doesNotContainKey(DlqHeaders.EXCEPTION_STACKTRACE);
    }

    @Test
    void generatesCorrelationIdAndDefaultsOriginalTopicWhenAbsent() {
        FailureContext ctx = factory.from(record("x"));

        assertThat(ctx.correlationId()).isNotBlank();
        assertThat(ctx.originalTopic()).isEqualTo("orders-topic");
    }

    @Test
    void tombstoneBecomesEmptyPayload() {
        assertThat(factory.from(record(null)).payload()).isEmpty();
    }

    @Test
    void eventIdIsStableForTheSameDlqRecord() {
        assertThat(factory.from(record("a")).eventId()).isEqualTo(factory.from(record("b")).eventId());
    }
}
