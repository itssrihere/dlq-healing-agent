package com.example.dlqagent.agent;

import com.example.dlqagent.domain.TopicAdapter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Component;

/** Resolves the adapter for a topic. A topic without an adapter cannot be repaired. */
@Component
public class TopicAdapterRegistry {

    private final Map<String, TopicAdapter> byTopic = new HashMap<>();

    public TopicAdapterRegistry(List<TopicAdapter> adapters) {
        for (TopicAdapter adapter : adapters) {
            if (byTopic.put(adapter.topic(), adapter) != null) {
                throw new IllegalStateException("Two topic adapters registered for topic '" + adapter.topic() + "'");
            }
        }
    }

    public Optional<TopicAdapter> forTopic(String topic) {
        return Optional.ofNullable(byTopic.get(topic));
    }
}
