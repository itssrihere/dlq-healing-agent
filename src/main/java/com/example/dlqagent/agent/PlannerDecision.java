package com.example.dlqagent.agent;

import com.example.dlqagent.domain.RemediationStrategy;

/**
 * Raw structured output requested from the LLM. This is untrusted: it is validated and mapped
 * into a {@code RemediationPlan} by {@link SpringAiRemediationPlanner} before anything acts on it.
 */
record PlannerDecision(RemediationStrategy strategy, double confidence, String rationale) {}
