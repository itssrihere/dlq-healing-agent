package com.example.dlqagent.agent;

import com.example.dlqagent.domain.Evaluation;
import com.example.dlqagent.domain.FieldChange;
import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.domain.PayloadRepairer;
import com.example.dlqagent.domain.RepairResult;
import com.example.dlqagent.domain.ToolCallRecord;
import com.example.dlqagent.domain.TopicAdapter;
import com.example.dlqagent.infrastructure.DlqAgentProperties;
import com.example.dlqagent.json.JsonDiff;
import com.example.dlqagent.json.StrictJson;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import com.example.dlqagent.observability.LogSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.support.ToolCallbacks;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

/**
 * Strategy A, generic over topics. The topic's {@link TopicAdapter} supplies the contract and the read-only tools
 * the model may call; the model returns a {@link PatchProposal}; Java applies it and the adapter's deterministic
 * {@link TopicAdapter#evaluate} has the final say. Every failure path returns
 * {@link RepairResult.Unrepairable} (never throws), which the triage service quarantines.
 */
@Component
final class SchemaRepairAgent implements PayloadRepairer {

    private static final Logger log = LoggerFactory.getLogger(SchemaRepairAgent.class);
    private static final int MAX_SUMMARY_CHARS = 500;

    private final ChatClient chatClient;
    private final TopicAdapterRegistry adapters;
    private final PatchApplier applier;
    private final String instructions;
    private final int maxPayloadChars;
    private final int maxStackTraceChars;

    SchemaRepairAgent(
            ChatClient.Builder builder,
            TopicAdapterRegistry adapters,
            PatchApplier applier,
            DlqAgentProperties props,
            @Value("classpath:prompts/repair-system.txt") Resource systemPrompt) {
        this.chatClient = builder.build();
        this.adapters = adapters;
        this.applier = applier;
        this.instructions = read(systemPrompt);
        this.maxPayloadChars = props.planner().maxPayloadChars();
        this.maxStackTraceChars = props.planner().maxStackTraceChars();
    }

    @Override
    public RepairResult repair(FailureContext ctx) {
        TopicAdapter adapter = adapters.forTopic(ctx.originalTopic()).orElse(null);
        if (adapter == null) {
            return new RepairResult.Unrepairable("No topic adapter registered for '" + ctx.originalTopic() + "'");
        }

        ObjectNode original;
        try {
            original = StrictJson.parseObject(ctx.payload());
        } catch (IllegalArgumentException e) {
            return new RepairResult.Unrepairable("Payload is " + e.getMessage() + "; only JSON objects can be repaired");
        }

        List<ToolCallRecord> toolCalls = new CopyOnWriteArrayList<>();
        PatchProposal proposal;
        try {
            // Instructions and contract are template parameters, so braces in them are never parsed as variables.
            proposal = chatClient.prompt()
                    .system(s -> s.text("{instructions}\n\n{contract}")
                            .param("instructions", instructions)
                            .param("contract", adapter.contract()))
                    .user(u -> u.text("{failure}").param("failure", FailurePrompt.render(
                            "Repair this failed event.", ctx, maxPayloadChars, maxStackTraceChars)))
                    .toolCallbacks(recording(adapter, toolCalls))
                    .call()
                    .entity(PatchProposal.class);
        } catch (RuntimeException e) {
            log.warn("Repair agent call failed: {}", LogSafe.clean(e));
            return new RepairResult.Unrepairable("Repair agent call failed: " + e.getClass().getSimpleName(),
                    null, null, toolCalls, List.of());
        }
        if (proposal == null) {
            return new RepairResult.Unrepairable("Repair agent returned no proposal", null, null, toolCalls, List.of());
        }
        String summary = FailurePrompt.truncate(proposal.summary(), MAX_SUMMARY_CHARS);
        double raw = proposal.confidence();
        if (!proposal.repairable()) {
            return new RepairResult.Unrepairable("Repair agent declined: " + summary, summary, raw, toolCalls, List.of());
        }

        ObjectNode patched;
        try {
            patched = applier.apply(adapter, original, proposal);
        } catch (PatchRejectedException e) {
            return new RepairResult.Unrepairable("Patch rejected: " + e.getMessage(), summary, raw, toolCalls, List.of());
        }
        if (patched.equals(original)) {
            return new RepairResult.Unrepairable("Patch changes nothing; the failure is not a payload problem",
                    summary, raw, toolCalls, List.of());
        }

        String candidate;
        try {
            candidate = StrictJson.write(patched);
        } catch (IllegalArgumentException e) {
            return new RepairResult.Unrepairable("Patched payload could not be serialized", summary, raw, toolCalls, List.of());
        }
        List<FieldChange> changes = JsonDiff.diff(original, patched);
        Evaluation evaluation = adapter.evaluate(ctx.payload(), candidate, ctx.key());
        if (evaluation.rejected()) {
            return new RepairResult.Unrepairable(
                    "Evaluator gate rejected patch: " + String.join("; ", evaluation.violations()),
                    summary, raw, toolCalls, changes);
        }

        List<String> flags = new ArrayList<>(evaluation.riskFlags());
        double confidence = raw;
        if (Double.isNaN(confidence) || confidence < 0.0 || confidence > 1.0) {
            flags.add("repair confidence was invalid (" + confidence + ")");
            confidence = 0.0;
        }
        return new RepairResult.Repaired(candidate, summary, flags, confidence, toolCalls, changes);
    }

    /** The adapter's tools, each wrapped so the audit trail can say which tools the model actually used. */
    private static ToolCallback[] recording(TopicAdapter adapter, List<ToolCallRecord> sink) {
        return Arrays.stream(ToolCallbacks.from(adapter.tools().toArray()))
                .map(t -> (ToolCallback) new RecordingToolCallback(t, sink))
                .toArray(ToolCallback[]::new);
    }

    private static String read(Resource resource) {
        try {
            return resource.getContentAsString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read repair system prompt", e);
        }
    }
}
