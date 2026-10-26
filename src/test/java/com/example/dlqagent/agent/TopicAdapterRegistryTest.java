package com.example.dlqagent.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.example.dlqagent.domain.TopicAdapter;
import java.util.List;
import org.junit.jupiter.api.Test;

class TopicAdapterRegistryTest {

    private static TopicAdapter adapterFor(String topic) {
        TopicAdapter a = mock(TopicAdapter.class);
        when(a.topic()).thenReturn(topic);
        return a;
    }

    @Test
    void resolvesByTopicAndReturnsEmptyForUnknownTopics() {
        var orders = adapterFor("orders-topic");
        var registry = new TopicAdapterRegistry(List.of(orders, adapterFor("payments-topic")));

        assertThat(registry.forTopic("orders-topic")).containsSame(orders);
        assertThat(registry.forTopic("nope")).isEmpty();
    }

    @Test
    void twoAdaptersForOneTopicFailFastAtStartup() {
        assertThatThrownBy(() -> new TopicAdapterRegistry(List.of(adapterFor("t"), adapterFor("t"))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("'t'");
    }
}
