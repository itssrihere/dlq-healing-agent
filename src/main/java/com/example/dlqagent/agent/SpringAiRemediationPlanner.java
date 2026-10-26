package com.example.dlqagent.agent;

import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.domain.RemediationPlan;
import com.example.dlqagent.domain.RemediationPlanner;
import com.example.dlqagent.infrastructure.DlqAgentProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

/**
 * LLM-backed {@link RemediationPlanner}. It only classifies; it has no tools and cannot act.
 * Depends on {@link ChatClient} alone, so the provider (OpenAI, Anthropic, Ollama) is a config choice.
 *
 * <p>Never throws: any model, parsing or validation problem yields a {@link RemediationPlan.Quarantine}.
 */
@Component
class SpringAiRemediationPlanner implements RemediationPlanner {

    private static final Logger log = LoggerFactory.getLogger(SpringAiRemediationPlanner.class);
    private static final int MAX_RATIONALE_CHARS = 500;

    private final ChatClient chatClient;
    private final double minConfidence;
    private final int maxPayloadChars;
    private final int maxStackTraceChars;

    SpringAiRemediationPlanner(
            ChatClient.Builder builder,
            DlqAgentProperties props,
            @Value("classpath:prompts/planner-system.txt") Resource systemPrompt) {
        // The system prompt is passed as a parameter, not template text, so it is never parsed for {placeholders}.
        this.chatClient = builder.defaultSystem(s -> s.text("{instructions}").param("instructions", read(systemPrompt))).build();
        this.minConfidence = props.planner().minConfidence();
        this.maxPayloadChars = props.planner().maxPayloadChars();
        this.maxStackTraceChars = props.planner().maxStackTraceChars();
    }

    @Override
    public RemediationPlan plan(FailureContext ctx) {
        try {
            // Payload text is a template parameter: JSON braces in it must not be read as template variables.
            PlannerDecision decision = chatClient.prompt()
                    .user(u -> u.text("{failure}").param("failure",
                            FailurePrompt.render("Classify this failed event.", ctx, maxPayloadChars, maxStackTraceChars)))
                    .call()
                    .entity(PlannerDecision.class);
            return toPlan(decision);
        } catch (RuntimeException e) {
            log.warn("Planner call failed, quarantining: {}", e.toString());
            return new RemediationPlan.Quarantine("Planner call failed: " + e.getClass().getSimpleName());
        }
    }

    private RemediationPlan toPlan(PlannerDecision d) {
        if (d == null || d.strategy() == null) {
            return new RemediationPlan.Quarantine("Planner returned no usable decision");
        }
        double confidence = d.confidence();
        if (Double.isNaN(confidence) || confidence < 0.0 || confidence > 1.0) {
            return new RemediationPlan.Quarantine("Planner returned invalid confidence: " + confidence);
        }
        String rationale = truncate(d.rationale() == null || d.rationale().isBlank() ? "no rationale given" : d.rationale(),
                MAX_RATIONALE_CHARS);
        if (confidence < minConfidence) {
            return new RemediationPlan.Quarantine(
                    "Low confidence (%.2f < %.2f) for %s: %s".formatted(confidence, minConfidence, d.strategy(), rationale));
        }
        return switch (d.strategy()) {
            case SCHEMA_DRIFT -> new RemediationPlan.SchemaRepair(rationale, confidence);
            case TRANSIENT -> new RemediationPlan.TransientRetry(rationale, confidence);
            case POISON_PILL -> new RemediationPlan.Quarantine(rationale);
        };
    }

    private static String truncate(String s, int max) {
        return FailurePrompt.truncate(s, max);
    }

    private static String read(Resource resource) {
        try {
            return resource.getContentAsString(java.nio.charset.StandardCharsets.UTF_8);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Cannot read planner system prompt", e);
        }
    }
}
