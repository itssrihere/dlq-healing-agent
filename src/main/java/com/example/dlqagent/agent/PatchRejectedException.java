package com.example.dlqagent.agent;

/** The proposal could not even be applied (bad value, forbidden or duplicate edit, too many edits). */
class PatchRejectedException extends RuntimeException {

    PatchRejectedException(String message) {
        super(message);
    }
}
