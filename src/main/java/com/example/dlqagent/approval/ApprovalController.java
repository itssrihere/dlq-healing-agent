package com.example.dlqagent.approval;

import com.example.dlqagent.domain.QuarantineStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.security.Principal;
import java.util.List;
import java.util.UUID;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Review queue for Tier 2 proposals. Authenticated; the caller's name is recorded as the reviewer.
 * The state-changing endpoints accept only {@code application/json} (see SecurityConfig for why).
 */
@RestController
@RequestMapping("/api/approvals")
class ApprovalController {

    record ApproveRequest(@Size(max = 1000) String notes) {}

    record RejectRequest(@NotBlank @Size(max = 1000) String notes) {}

    private final ApprovalService service;

    ApprovalController(ApprovalService service) {
        this.service = service;
    }

    @GetMapping
    List<ApprovalView> list(
            @RequestParam(defaultValue = "PENDING_HUMAN_APPROVAL") QuarantineStatus status,
            @RequestParam(defaultValue = "50") int limit) {
        return service.list(status, limit).stream().map(ApprovalView::of).toList();
    }

    @GetMapping("/{id}")
    ApprovalView get(@PathVariable UUID id) {
        return ApprovalView.of(service.get(id));
    }

    @PostMapping(path = "/{id}/approve", consumes = MediaType.APPLICATION_JSON_VALUE)
    ApprovalView approve(@PathVariable UUID id, @RequestBody @Valid ApproveRequest body, Principal reviewer) {
        // The body is required even though "notes" is optional: a request with no body and no content type is a
        // "simple" cross-site request that a browser sends without a CORS preflight, so it must never match.
        return ApprovalView.of(service.approve(id, reviewer.getName(), body.notes()));
    }

    @PostMapping(path = "/{id}/reject", consumes = MediaType.APPLICATION_JSON_VALUE)
    ResponseEntity<ApprovalView> reject(@PathVariable UUID id, @RequestBody @Valid RejectRequest body,
                                        Principal reviewer) {
        return ResponseEntity.ok(ApprovalView.of(service.reject(id, reviewer.getName(), body.notes())));
    }
}
