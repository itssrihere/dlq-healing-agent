package com.example.dlqagent.messaging;

import com.example.dlqagent.domain.EventReplayer;
import com.example.dlqagent.domain.ReplayRequest;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes a repaired event back to its original topic, with the original key and headers plus audit headers.
 * Delivery is at-least-once: the consumer of the main topic must be idempotent (orders.order_id is the primary key).
 */
@Component
class KafkaEventReplayer implements EventReplayer {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventReplayer.class);
    private static final long SEND_TIMEOUT_SECONDS = 10;
    private static final Set<String> AUDIT_HEADERS = Set.of(DlqHeaders.REPAIRED_BY_AGENT, DlqHeaders.REPAIR_SUMMARY,
            DlqHeaders.CORRELATION_ID, DlqHeaders.APPROVED_BY);

    private final KafkaTemplate<String, String> kafka;
    private final ObservationRegistry observations;

    KafkaEventReplayer(KafkaTemplate<String, String> kafka, ObservationRegistry observations) {
        this.kafka = kafka;
        this.observations = observations;
    }

    @Override
    public void replay(ReplayRequest request) {
        Observation.createNotStarted("dlq.replay", observations)
                .lowCardinalityKeyValue("topic", request.topic())
                .lowCardinalityKeyValue("approved", String.valueOf(request.approvedBy() != null))
                .highCardinalityKeyValue("correlation.id", request.correlationId())
                .observe(() -> send(request));
    }

    private void send(ReplayRequest request) {
        ProducerRecord<String, String> record = new ProducerRecord<>(request.topic(), request.key(), request.payload());
        request.headers().forEach((name, value) -> {
            // Audit headers are ours alone; drop any copies carried by the failed event.
            if (!AUDIT_HEADERS.contains(name)) {
                record.headers().add(name, bytes(value));
            }
        });
        record.headers().add(DlqHeaders.CORRELATION_ID, bytes(request.correlationId()));
        record.headers().add(DlqHeaders.REPAIRED_BY_AGENT, bytes("true"));
        record.headers().add(DlqHeaders.REPAIR_SUMMARY, bytes(request.summary() == null ? "" : request.summary()));
        if (request.approvedBy() != null) {
            record.headers().add(DlqHeaders.APPROVED_BY, bytes(request.approvedBy()));
        }

        try {
            var result = kafka.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("Replayed repaired event topic={} partition={} offset={} approvedBy={}", request.topic(),
                    result.getRecordMetadata().partition(), result.getRecordMetadata().offset(), request.approvedBy());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while replaying event", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Replay was not acknowledged by the broker", e);
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
