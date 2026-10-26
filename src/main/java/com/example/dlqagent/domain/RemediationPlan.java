package com.example.dlqagent.domain;

/**
 * The planner's decision. Sealed so a {@code switch} over it is exhaustive: adding a
 * strategy forces every handler to be updated at compile time.
 */
public sealed interface RemediationPlan {

    /** Strategy A. */
    record SchemaRepair(String rationale, double confidence) implements RemediationPlan {}

    /** Strategy B. */
    record TransientRetry(String rationale, double confidence) implements RemediationPlan {}

    /** Strategy C (also the fail-closed fallback). */
    record Quarantine(String reason) implements RemediationPlan {}
}
