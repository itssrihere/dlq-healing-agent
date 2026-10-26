package com.example.dlqagent.domain;

/** The three operational strategies the planner can route a failure into. */
public enum RemediationStrategy {
    /** A: data format / schema drift; repair the payload and replay. */
    SCHEMA_DRIFT,
    /** B: transient infrastructure failure; retry with exponential backoff. */
    TRANSIENT,
    /** C: poison pill / unfixable; quarantine for human audit. */
    POISON_PILL
}
