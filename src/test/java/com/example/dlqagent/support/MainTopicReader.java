package com.example.dlqagent.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Reads what the agent replayed to orders-topic, as a downstream consumer would. */
public final class MainTopicReader {

    private MainTopicReader() {}

    /** Polls until a record with the key shows up or the time is up; returns what was found (empty if none). */
    public static List<ConsumerRecord<String, String>> read(String bootstrapServers, String key, Duration atMost) {
        return read(bootstrapServers, "orders-topic", key, atMost);
    }

    public static List<ConsumerRecord<String, String>> read(String bootstrapServers, String topic, String key, Duration atMost) {
        var props = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + atMost.toNanos();
            while (System.nanoTime() < deadline && found.isEmpty()) {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    if (key.equals(r.key())) found.add(r);
                });
            }
        }
        return found;
    }

    /** Reads for the whole period and returns every record with the key: for asserting that nothing was sent twice. */
    public static List<ConsumerRecord<String, String>> readAll(String bootstrapServers, String key, Duration period) {
        return readAll(bootstrapServers, "orders-topic", key, period);
    }

    public static List<ConsumerRecord<String, String>> readAll(String bootstrapServers, String topic, String key, Duration period) {
        var props = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + period.toNanos();
            while (System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(300)).forEach(r -> {
                    if (key.equals(r.key())) found.add(r);
                });
            }
        }
        return found;
    }

    public static String header(ConsumerRecord<?, ?> r, String name) {
        var h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), java.nio.charset.StandardCharsets.UTF_8);
    }
}
