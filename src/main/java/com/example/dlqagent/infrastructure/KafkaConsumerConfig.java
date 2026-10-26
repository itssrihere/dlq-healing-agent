package com.example.dlqagent.infrastructure;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
class KafkaConsumerConfig {

    /**
     * A DLQ record must never be skipped because of a downstream problem (e.g. Postgres down).
     * Retries are unbounded with capped exponential backoff; the offset is not committed meanwhile.
     */
    @Bean
    DefaultErrorHandler dlqListenerErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(30_000L);
        return new DefaultErrorHandler(backOff);
    }
}
