package com.example.dlqagent.approval;

/** The request is valid but the event is not in a state that allows it (already reviewed, no longer passes the gate). */
class ApprovalConflictException extends RuntimeException {

    ApprovalConflictException(String message) {
        super(message);
    }
}
