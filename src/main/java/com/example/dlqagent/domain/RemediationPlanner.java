package com.example.dlqagent.domain;

/** Port: decides which strategy handles a failure. Implementations must never throw for bad model output. */
public interface RemediationPlanner {

    RemediationPlan plan(FailureContext context);
}
