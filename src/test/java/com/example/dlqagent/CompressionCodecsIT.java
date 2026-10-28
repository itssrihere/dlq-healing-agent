package com.example.dlqagent;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.dlqagent.json.StrictJson;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Sends a compressible record through the real broker with every Kafka compression codec and reads it back.
 *
 * <p>Why this exists: the codec libraries (lz4-java, snappy-java, zstd-jni) are transitive dependencies of the Kafka
 * client that nothing else in the test suite touches, because the application does not compress its own records.
 * They are exactly the kind of dependency that gets security upgrades (and Dependabot bumps), and without this test a
 * green build would say nothing about whether such an upgrade still loads and works. A DLQ producer elsewhere in the
 * system may well compress, and this service must be able to read what it writes.
 */
class CompressionCodecsIT extends AbstractIntegrationTest {

    /** Large and repetitive, so the codec really compresses the batch instead of storing it. */
    private static final String PAYLOAD = "{\"orderId\":\"o-1\",\"note\":\"" + "compress me ".repeat(4000) + "\"}";

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"gzip", "snappy", "lz4", "zstd"})
    void aRecordSurvivesARoundTripThroughTheBrokerWithEachCodec(String codec) throws Exception {
        String topic = "codec-" + codec + "-" + UUID.randomUUID();

        try (var producer = new KafkaProducer<String, String>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.COMPRESSION_TYPE_CONFIG, codec,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            producer.send(new ProducerRecord<>(topic, "k", PAYLOAD)).get();
        }

        try (var consumer = new KafkaConsumer<String, String>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "codec-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(topic));
            ConsumerRecord<String, String> received = null;
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (received == null && System.nanoTime() < deadline) {
                for (var r : consumer.poll(Duration.ofMillis(500))) {
                    received = r;
                }
            }
            assertThat(received).as("a record compressed with " + codec + " must be readable").isNotNull();
            assertThat(received.value()).isEqualTo(PAYLOAD);
        }
    }

    @Test
    void theCodecLibrariesAreAtOrAboveTheVersionsWeRequireForSecurity() throws Exception {
        // Fails loudly if a dependency override is dropped and an older, vulnerable version silently returns.
        assertThat(atLeast(artifactVersion("org.xerial.snappy", "snappy-java"), "1.1.10.9")).isTrue();
        assertThat(atLeast(artifactVersion("at.yawk.lz4", "lz4-java"), "1.11.4")).isTrue();
    }

    /** The resolved version recorded in the build's own SBOM (the same file the vulnerability scan reads). */
    private static String artifactVersion(String groupId, String artifactId) throws Exception {
        try (var in = CompressionCodecsIT.class.getResourceAsStream("/META-INF/sbom/application.cdx.json")) {
            assertThat(in).as("the build's SBOM is on the classpath").isNotNull();
            for (var component : StrictJson.parseObject(new String(in.readAllBytes())).withArray("components")) {
                if (groupId.equals(component.path("group").asText()) && artifactId.equals(component.path("name").asText())) {
                    return component.path("version").asText();
                }
            }
        }
        throw new AssertionError(groupId + ":" + artifactId + " is not in the SBOM");
    }

    /** Numeric, segment by segment: 1.12.1 is above 1.11.4 and 1.1.10.12 is above 1.1.10.9. */
    private static boolean atLeast(String actual, String minimum) {
        String[] a = actual.split("\\.");
        String[] m = minimum.split("\\.");
        for (int i = 0; i < Math.max(a.length, m.length); i++) {
            int x = i < a.length ? Integer.parseInt(a[i]) : 0;
            int y = i < m.length ? Integer.parseInt(m[i]) : 0;
            if (x != y) {
                return x > y;
            }
        }
        return true;
    }
}
