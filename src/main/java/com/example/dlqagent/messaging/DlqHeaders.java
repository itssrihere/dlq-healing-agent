package com.example.dlqagent.messaging;

import com.example.dlqagent.domain.FailureContext;
import org.springframework.kafka.support.KafkaHeaders;

/** Header names used on DLQ and replayed records. */
public final class DlqHeaders {

    /** Propagated end to end so one event can be traced across logs and tables. */
    public static final String CORRELATION_ID = "x-correlation-id";

    /** Added on events the agent repaired and replayed. */
    public static final String REPAIRED_BY_AGENT = FailureContext.REPAIRED_BY_AGENT_HEADER;

    /** The human who approved a Tier 2 repair; absent on Tier 1 automatic replays. */
    public static final String APPROVED_BY = "x-approved-by";

    /** Short human-readable description of what the agent changed. */
    public static final String REPAIR_SUMMARY = "x-repair-summary";

    /** Standard Spring Kafka dead-letter headers (written by DeadLetterPublishingRecoverer). */
    public static final String ORIGINAL_TOPIC = KafkaHeaders.DLT_ORIGINAL_TOPIC;
    public static final String EXCEPTION_STACKTRACE = KafkaHeaders.DLT_EXCEPTION_STACKTRACE;

    /** Prefix of Spring's binary dead-letter bookkeeping headers; not meaningful as text. */
    static final String DLT_PREFIX = "kafka_dlt-";

    private DlqHeaders() {}
}
