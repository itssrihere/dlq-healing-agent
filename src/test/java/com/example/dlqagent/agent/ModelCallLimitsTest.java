package com.example.dlqagent.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.model.openai.autoconfigure.OpenAiChatAutoConfiguration;
import org.springframework.ai.model.tool.autoconfigure.ToolCallingAutoConfiguration;
import org.springframework.ai.retry.autoconfigure.SpringAiRetryAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.http.client.HttpClientAutoConfiguration;
import org.springframework.boot.autoconfigure.web.client.RestClientAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * What a slow or flaky model endpoint can do to the service. The Spring AI defaults (10 attempts, a 2 s first delay
 * multiplied by 5 each time, no cap, and no HTTP read timeout) let one bad call block a stream's listener for many
 * minutes. These tests run the real Spring AI and Spring Boot auto-configuration against a local server that is slow, and
 * check the limits configured in application.yml: a capped retry and an HTTP read timeout.
 */
class ModelCallLimitsTest {

    private static final String OK = "{\"id\":\"x\",\"object\":\"chat.completion\",\"created\":1,\"model\":\"m\","
            + "\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"OK\"},\"finish_reason\":\"stop\"}],"
            + "\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}";

    private HttpServer server;
    private final AtomicInteger requests = new AtomicInteger();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    /** A model endpoint that answers every request after {@code delayMs}. */
    private String slowEndpoint(long delayMs) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.incrementAndGet();
            try {
                Thread.sleep(delayMs);
                byte[] body = OK.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            } catch (IOException | InterruptedException ignored) {
                // the client gave up before the answer: exactly what the timeout is for
            } finally {
                exchange.close();
            }
        });
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.start();
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private ApplicationContextRunner runner(String baseUrl, String... properties) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(HttpClientAutoConfiguration.class, RestClientAutoConfiguration.class,
                        SpringAiRetryAutoConfiguration.class, ToolCallingAutoConfiguration.class, OpenAiChatAutoConfiguration.class))
                .withPropertyValues("spring.ai.openai.api-key=test-key", "spring.ai.openai.base-url=" + baseUrl)
                .withPropertyValues(properties);
    }

    @Test
    void withoutAReadTimeoutACallJustWaitsForASlowEndpoint() throws Exception {
        // the control: the same slow endpoint, no limit configured, so the client waits and then succeeds
        String url = slowEndpoint(2_500);

        runner(url).run(ctx -> {
            long start = System.nanoTime();
            String answer = ctx.getBean(ChatModel.class).call("hi");
            assertThat(answer).isEqualTo("OK");
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(2_400));
        });
    }

    @Test
    void aReadTimeoutCutsASlowCallOffAndTheRetryIsBounded() throws Exception {
        String url = slowEndpoint(6_000);

        runner(url, "spring.http.client.read-timeout=1s", "spring.ai.retry.max-attempts=2",
                "spring.ai.retry.backoff.initial-interval=100ms", "spring.ai.retry.backoff.multiplier=2",
                "spring.ai.retry.backoff.max-interval=500ms").run(ctx -> {
            long start = System.nanoTime();
            assertThatThrownBy(() -> ctx.getBean(ChatModel.class).call("hi")).isInstanceOf(RuntimeException.class);
            Duration took = Duration.ofNanos(System.nanoTime() - start);

            assertThat(took).as("2 attempts of 1 s plus a short backoff, not the 6 s the server needs").isLessThan(Duration.ofSeconds(5));
            assertThat(requests.get()).as("both attempts were made and no more").isEqualTo(2);
        });
    }

    @Test
    void theLimitsInApplicationYmlAreTheOnesWeMeanTo() throws IOException {
        var yml = new org.springframework.boot.env.YamlPropertySourceLoader()
                .load("application", new org.springframework.core.io.ClassPathResource("application.yml")).get(0);

        assertThat(yml.getProperty("spring.ai.retry.max-attempts")).isEqualTo(3);
        assertThat(yml.getProperty("spring.ai.retry.backoff.max-interval")).isEqualTo("10s");
        assertThat(yml.getProperty("spring.http.client.read-timeout")).isEqualTo("30s");
        assertThat(yml.getProperty("spring.http.client.connect-timeout")).isEqualTo("5s");
        assertThat(yml.getProperty("spring.kafka.consumer.properties.max.poll.interval.ms")).isEqualTo(600000);
    }
}
