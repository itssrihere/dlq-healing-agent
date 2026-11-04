package com.example.dlqagent.messaging;

import com.example.dlqagent.agent.DlqTriageService;
import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.observability.LogSafe;
import com.example.dlqagent.stream.HealingStream;
import com.example.dlqagent.stream.StreamRegistry;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes every stream's DLQ, one listener container per stream, each in its own consumer group. A container that
 * is stuck retrying (the database is down, say) blocks only its own DLQ's records, never another stream's.
 *
 * <p>The offset is committed only after triage succeeds, so a crash or a database outage causes redelivery rather
 * than loss. Triage is idempotent per record. The containers come from Spring Boot's own listener container factory,
 * so manual acknowledgement, the unbounded-retry error handler and trace propagation are the configured ones.
 */
@Component
class StreamListeners implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(StreamListeners.class);
    static final String MDC_CORRELATION_ID = "correlationId";
    static final String MDC_STREAM = "stream";

    private final StreamRegistry streams;
    private final FailureContextFactory contextFactory;
    private final DlqTriageService triage;
    private final ConcurrentKafkaListenerContainerFactory<Object, Object> factory;
    private final List<ConcurrentMessageListenerContainer<Object, Object>> containers = new ArrayList<>();
    private volatile boolean running;

    StreamListeners(StreamRegistry streams, FailureContextFactory contextFactory, DlqTriageService triage,
                    ConcurrentKafkaListenerContainerFactory<Object, Object> factory) {
        this.streams = streams;
        this.contextFactory = contextFactory;
        this.triage = triage;
        this.factory = factory;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        for (HealingStream stream : streams.all()) {
            ConcurrentMessageListenerContainer<Object, Object> container = factory.createContainer(stream.dlqTopic());
            container.getContainerProperties().setGroupId(stream.groupId());
            container.getContainerProperties().setMessageListener(
                    (AcknowledgingMessageListener<Object, Object>) (record, ack) -> onRecord(stream, record, ack));
            container.setBeanName("dlq-" + stream.name());
            container.start();
            containers.add(container);
            log.info("Healing stream '{}': {} -> {} (group {})", LogSafe.clean(stream.name()),
                    LogSafe.clean(stream.dlqTopic()), LogSafe.clean(stream.sourceTopic()), LogSafe.clean(stream.groupId()));
        }
        running = true;
    }

    @SuppressWarnings("unchecked")
    void onRecord(HealingStream stream, ConsumerRecord<Object, Object> raw, Acknowledgment ack) {
        ConsumerRecord<String, String> record = (ConsumerRecord<String, String>) (ConsumerRecord<?, ?>) raw;
        FailureContext ctx = contextFactory.from(stream, record);
        MDC.put(MDC_STREAM, stream.name());
        MDC.put(MDC_CORRELATION_ID, ctx.correlationId());
        try {
            log.info("Received DLQ event topic={} partition={} offset={}",
                    LogSafe.clean(record.topic()), record.partition(), record.offset());
            triage.triage(stream, ctx);
            ack.acknowledge();
        } finally {
            MDC.remove(MDC_CORRELATION_ID);
            MDC.remove(MDC_STREAM);
        }
    }

    @Override
    public synchronized void stop() {
        containers.forEach(ConcurrentMessageListenerContainer::stop);
        containers.clear();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
