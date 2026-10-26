package com.example.dlqagent.audit;

import com.example.dlqagent.domain.AuditEntry;
import com.example.dlqagent.domain.AuditLog;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Read-only view of the audit trail for one event, found by its correlation ID. Same authentication as approvals. */
@RestController
@RequestMapping("/api/audit")
class AuditController {

    private static final int MAX_LIMIT = 200;

    private final AuditLog audit;

    AuditController(AuditLog audit) {
        this.audit = audit;
    }

    @GetMapping
    List<AuditEntry> byCorrelationId(@RequestParam String correlationId, @RequestParam(defaultValue = "50") int limit) {
        return audit.findByCorrelationId(correlationId, Math.max(1, Math.min(limit, MAX_LIMIT)));
    }
}
