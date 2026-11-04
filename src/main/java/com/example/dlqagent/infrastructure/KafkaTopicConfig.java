package com.example.dlqagent.infrastructure;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Declares every stream's topics so they exist locally and in tests. Wherever topics are managed elsewhere (a real
 * cluster), set {@code dlqagent.topics.declare=false}: a service that creates its own source topics is a surprise.
 */
@Configuration
@ConditionalOnProperty(name = "dlqagent.topics.declare", havingValue = "true", matchIfMissing = true)
class KafkaTopicConfig {

    @Bean
    KafkaAdmin.NewTopics streamTopics(DlqAgentProperties props) {
        List<NewTopic> topics = new ArrayList<>();
        for (DlqAgentProperties.StreamConfig s : props.streams()) {
            for (String name : List.of(s.sourceTopic(), s.dlqTopic())) {
                topics.add(TopicBuilder.name(name).partitions(props.topics().partitions()).replicas(1).build());
            }
        }
        return new KafkaAdmin.NewTopics(topics.toArray(new NewTopic[0]));
    }
}
