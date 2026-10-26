package com.example.dlqagent.support;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/** Collects every finished span in memory so tests can assert on real traces (nothing is sent over the network). */
@TestConfiguration
public class TracingTestConfig {

    @Bean
    InMemorySpanExporter inMemorySpanExporter() {
        return InMemorySpanExporter.create();
    }
}
