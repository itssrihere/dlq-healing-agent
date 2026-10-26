package com.example.dlqagent.approval;

/** Approving failed because the broker did not acknowledge the replay; nothing was changed. */
class ReplayFailedException extends RuntimeException {

    ReplayFailedException(Throwable cause) {
        super("The replay was not acknowledged by Kafka; the event is still pending approval", cause);
    }
}
