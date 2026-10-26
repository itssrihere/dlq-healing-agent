package com.example.dlqagent.infrastructure;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Declares the topics so they exist locally and in tests. Production would manage these externally. */
@Configuration
class KafkaTopicConfig {

    @Bean
    NewTopic ordersTopic(DlqAgentProperties props) {
        return TopicBuilder.name(props.topics().main()).partitions(props.topics().partitions()).replicas(1).build();
    }

    @Bean
    NewTopic ordersDlqTopic(DlqAgentProperties props) {
        return TopicBuilder.name(props.topics().dlq()).partitions(props.topics().partitions()).replicas(1).build();
    }
}
