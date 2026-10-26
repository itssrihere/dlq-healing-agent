package com.example.dlqagent.messaging;

import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.infrastructure.DlqAgentProperties;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.stereotype.Component;

/** Maps a raw DLQ {@link ConsumerRecord} into a validated {@link FailureContext}. */
@Component
public class FailureContextFactory {

    private static final Pattern SAFE_CORRELATION_ID = Pattern.compile("[A-Za-z0-9._:-]{1,64}");

    private final String defaultOriginalTopic;

    public FailureContextFactory(DlqAgentProperties props) {
        this.defaultOriginalTopic = props.topics().main();
    }

    public FailureContext from(ConsumerRecord<String, String> record) {
        // The correlation id ends up in logs, MDC and a bounded column. It comes from a producer, so it is only
        // trusted if it is short and made of harmless characters; otherwise a fresh one is generated.
        String correlationId = headerText(record, DlqHeaders.CORRELATION_ID);
        if (correlationId == null || !SAFE_CORRELATION_ID.matcher(correlationId).matches()) {
            correlationId = UUID.randomUUID().toString();
        }
        String originalTopic = headerText(record, DlqHeaders.ORIGINAL_TOPIC);
        if (originalTopic == null || originalTopic.isBlank()) {
            originalTopic = defaultOriginalTopic;
        }

        Map<String, String> headers = new LinkedHashMap<>();
        for (Header h : record.headers()) {
            if (h.value() != null && !h.key().startsWith(DlqHeaders.DLT_PREFIX)) {
                headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
            }
        }

        return new FailureContext(
                correlationId,
                record.topic(),
                record.partition(),
                record.offset(),
                originalTopic,
                record.key(),
                record.value(),
                headers,
                headerText(record, DlqHeaders.EXCEPTION_STACKTRACE),
                Instant.ofEpochMilli(record.timestamp()));
    }

    private static String headerText(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
