package com.example.dlqagent.observability;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/** Reads the current trace id so audit rows can be joined to the distributed trace. */
@Component
public class TraceContext {

    private final ObjectProvider<Tracer> tracer;

    public TraceContext(ObjectProvider<Tracer> tracer) {
        this.tracer = tracer;
    }

    /** @return the current trace id, or null if tracing is off or there is no active span */
    public String traceId() {
        Tracer t = tracer == null ? null : tracer.getIfAvailable();
        Span span = t == null ? null : t.currentSpan();
        return span == null ? null : span.context().traceId();
    }
}
