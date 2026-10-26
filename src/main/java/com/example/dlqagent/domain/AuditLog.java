package com.example.dlqagent.domain;

import java.util.List;

/** Port: append-only record of every execution. */
public interface AuditLog {

    /** Idempotent on {@link AuditEntry#id()}. @return true if a new row was written */
    boolean record(AuditEntry entry);

    /** Oldest first. */
    List<AuditEntry> findByCorrelationId(String correlationId, int limit);
}
