package com.example.dlqagent.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.dlqagent.infrastructure.DlqAgentProperties;
import com.example.dlqagent.infrastructure.DlqAgentProperties.Contract;
import com.example.dlqagent.infrastructure.DlqAgentProperties.StreamConfig;
import com.example.dlqagent.support.TestProps;
import com.example.dlqagent.support.TestStreams;
import jakarta.validation.Validation;
import java.util.List;
import org.junit.jupiter.api.Test;

class StreamRegistryTest {

    private static final Contract ORDERS = TestProps.ordersStream().contract();
    private static final Contract SHIPMENTS = new Contract("classpath:contracts/shipments.schema.json",
            List.of("shipmentId", "orderRef"), "shipmentId", List.of(), List.of());

    private static StreamConfig stream(String name, String dlq, String source, String group, Contract contract) {
        return new StreamConfig(name, dlq, source, group, contract, null, null, null);
    }

    private static DlqAgentProperties with(StreamConfig... streams) {
        DlqAgentProperties d = TestProps.defaults();
        return new DlqAgentProperties(d.topics(), d.retry(), d.planner(), d.gate(), d.approval(), List.of(streams));
    }

    // ---- configuration rules that stop the service at start-up ------------------------------------------------------

    @Test
    void twoStreamsCannotShareAName() {
        assertThatThrownBy(() -> with(stream("a", "a-dlq", "a-topic", null, ORDERS), stream("a", "b-dlq", "b-topic", null, ORDERS)))
                .hasMessageContaining("duplicate stream name 'a'");
    }

    @Test
    void twoStreamsCannotShareADlqTopicOrASourceTopic() {
        assertThatThrownBy(() -> with(stream("a", "x-dlq", "a-topic", null, ORDERS), stream("b", "x-dlq", "b-topic", null, ORDERS)))
                .hasMessageContaining("duplicate DLQ topic 'x-dlq'");
        assertThatThrownBy(() -> with(stream("a", "a-dlq", "x-topic", null, ORDERS), stream("b", "b-dlq", "x-topic", null, ORDERS)))
                .hasMessageContaining("duplicate source topic 'x-topic'");
    }

    @Test
    void twoStreamsCannotShareAConsumerGroup() {
        assertThatThrownBy(() -> with(stream("a", "a-dlq", "a-topic", "g", ORDERS), stream("b", "b-dlq", "b-topic", "g", ORDERS)))
                .hasMessageContaining("duplicate consumer group id 'g'");
    }

    @Test
    void aTopicCannotBeOneStreamsDlqAndAnotherStreamsSource() {
        // otherwise the healer would feed on its own output
        assertThatThrownBy(() -> with(stream("a", "a-dlq", "a-topic", null, ORDERS), stream("b", "c-dlq", "a-dlq", null, ORDERS)))
                .hasMessageContaining("cannot be both a DLQ and a source topic");
    }

    @Test
    void aStreamsDlqAndSourceTopicMustDiffer() {
        assertThatThrownBy(() -> stream("a", "same", "same", null, ORDERS)).hasMessageContaining("must differ");
    }

    @Test
    void streamNamesAreRestrictedToAHarmlessAlphabet() {
        // the name ends up in a bounded database column, logs and metrics tags
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var validator = factory.getValidator();
            assertThat(validator.validate(stream("orders-2", "a", "b", null, ORDERS))).isEmpty();
            assertThat(validator.validate(stream("Orders", "a", "b", null, ORDERS))).isNotEmpty();
            assertThat(validator.validate(stream("a b", "a", "b", null, ORDERS))).isNotEmpty();
            assertThat(validator.validate(stream("x".repeat(70), "a", "b", null, ORDERS))).isNotEmpty();
        }
    }

    @Test
    void theConsumerGroupDefaultsToOneNamedAfterTheStream() {
        assertThat(stream("payments", "p-dlq", "p-topic", null, ORDERS).groupIdOrDefault()).isEqualTo("dlq-healing-agent-payments");
        assertThat(stream("orders", "o-dlq", "o-topic", "dlq-healing-agent", ORDERS).groupIdOrDefault()).isEqualTo("dlq-healing-agent");
    }

    // ---- building the streams ----------------------------------------------------------------------------------------

    @Test
    void everyStreamGetsItsOwnAdapterContractAndCircuitBreaker() {
        var registry = TestStreams.registry(with(TestProps.ordersStream(),
                stream("shipments", "shipments-dlq", "shipments-topic", null, SHIPMENTS)));

        HealingStream orders = registry.byName("orders").orElseThrow();
        HealingStream shipments = registry.byName("shipments").orElseThrow();

        assertThat(registry.all()).extracting(HealingStream::name).containsExactly("orders", "shipments");
        assertThat(registry.forDlqTopic("shipments-dlq")).containsSame(shipments);
        assertThat(registry.forDlqTopic("nope")).isEmpty();
        assertThat(orders.breaker()).isNotSameAs(shipments.breaker());
        assertThat(orders.adapter().topic()).isEqualTo("orders-topic");
        assertThat(shipments.adapter().topic()).isEqualTo("shipments-topic");
        assertThat(shipments.adapter().editableFields()).containsOnlyKeys("carrier", "quantity", "shippedOn");
        assertThat(orders.adapter().contract()).doesNotContain("shipmentId");
    }

    @Test
    void streamSettingsOverrideTheGlobalDefaultsAndFallBackToThem() {
        var tuned = new StreamConfig("tuned", "t-dlq", "t-topic", null, SHIPMENTS, false, 0.5, 2);
        var registry = TestStreams.registry(with(TestProps.ordersStream(), tuned));

        HealingStream defaults = registry.byName("orders").orElseThrow();
        HealingStream custom = registry.byName("tuned").orElseThrow();

        assertThat(defaults.autoReplay()).isTrue();
        assertThat(defaults.autoReplayMinConfidence()).isEqualTo(0.9); // gate.auto-replay-min-confidence
        assertThat(defaults.maxAttempts()).isEqualTo(5);               // retry.max-attempts
        assertThat(custom.autoReplay()).isFalse();
        assertThat(custom.autoReplayMinConfidence()).isEqualTo(0.5);
        assertThat(custom.maxAttempts()).isEqualTo(2);
    }

    @Test
    void aBrokenStreamStopsTheServiceAndNamesItself() {
        var broken = new Contract("classpath:contracts/shipments.schema.json", List.of("noSuchField"), null, List.of(), List.of());

        assertThatThrownBy(() -> TestStreams.registry(with(TestProps.ordersStream(),
                stream("shipments", "s-dlq", "s-topic", null, broken))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Stream 'shipments' cannot start")
                .hasMessageContaining("identity field 'noSuchField'");
    }

    @Test
    void aMissingSchemaFileAlsoNamesTheStream() {
        var missing = new Contract("classpath:contracts/nope.schema.json", List.of("id"), null, List.of(), List.of());

        assertThatThrownBy(() -> TestStreams.registry(with(stream("ghost", "g-dlq", "g-topic", null, missing))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Stream 'ghost' cannot start");
    }
}
