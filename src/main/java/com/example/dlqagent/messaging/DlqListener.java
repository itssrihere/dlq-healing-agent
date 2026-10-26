package com.example.dlqagent.messaging;

import com.example.dlqagent.agent.DlqTriageService;
import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.observability.LogSafe;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes the DLQ. The offset is committed only after triage succeeds, so a crash or a
 * database outage causes redelivery rather than loss. Triage is idempotent per record.
 */
@Component
class DlqListener {

    private static final Logger log = LoggerFactory.getLogger(DlqListener.class);
    static final String MDC_CORRELATION_ID = "correlationId";

    private final FailureContextFactory contextFactory;
    private final DlqTriageService triage;

    DlqListener(FailureContextFactory contextFactory, DlqTriageService triage) {
        this.contextFactory = contextFactory;
        this.triage = triage;
    }

    @KafkaListener(topics = "${dlqagent.topics.dlq}")
    void onDlqRecord(ConsumerRecord<String, String> record, Acknowledgment ack) {
        FailureContext ctx = contextFactory.from(record);
        MDC.put(MDC_CORRELATION_ID, ctx.correlationId());
        try {
            log.info("Received DLQ event topic={} partition={} offset={}",
                    LogSafe.clean(record.topic()), record.partition(), record.offset());
            triage.triage(ctx);
            ack.acknowledge();
        } finally {
            MDC.remove(MDC_CORRELATION_ID);
        }
    }
}
