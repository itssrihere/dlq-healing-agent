package com.example.dlqagent.domain;

import java.time.Instant;

/** What recording a transient failure led to. */
public sealed interface RetryDecision {

    /** A replay is (or already was) scheduled. {@code replayNumber} counts from 1. */
    record Scheduled(int replayNumber, int maxReplays, Instant at, boolean duplicate) implements RetryDecision {}

    /** The attempt budget is spent; the caller quarantines the event. */
    record Exhausted(String reason) implements RetryDecision {}

    /** This exact DLQ record was already processed (redelivery); change nothing. */
    record AlreadyHandled() implements RetryDecision {}
}
