package com.example.dlqagent.approval;

import java.util.UUID;

class ApprovalNotFoundException extends RuntimeException {

    ApprovalNotFoundException(UUID id) {
        super("No quarantined event with id " + id);
    }
}
