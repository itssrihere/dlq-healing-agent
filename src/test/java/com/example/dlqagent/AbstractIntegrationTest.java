package com.example.dlqagent;

import com.example.dlqagent.support.StubLlmConfig;
import com.example.dlqagent.support.TracingTestConfig;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for end-to-end tests. Containers are static and shared across all subclasses,
 * so Kafka and Postgres start once per JVM. Connection settings are wired by @ServiceConnection.
 */
// MockMvc lives here, not on a subclass: a differently configured subclass would build a second Spring context,
// and cached contexts stay alive, so two DLQ listeners in one consumer group would split the events between them.
@SpringBootTest
@AutoConfigureMockMvc
@AutoConfigureObservability // Spring Boot tests switch tracing off by default; the audit trail needs real trace ids
@Import({StubLlmConfig.class, TracingTestConfig.class})
@ActiveProfiles("test")
public abstract class AbstractIntegrationTest {

    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void overrides(DynamicPropertyRegistry registry) {
        // StubLlmConfig replaces the ChatModel, so no real call is made; the key only has to be non-empty for
        // auto-configuration. The opt-in live evaluation (DLQ_LIVE_LLM=true) uses the real key instead.
        registry.add("dlqagent.approval.password", () -> "test-reviewer-password");
        registry.add("spring.ai.openai.api-key",
                () -> "true".equals(System.getenv("DLQ_LIVE_LLM")) ? System.getenv("OPENAI_API_KEY") : "test-key");
    }
}
