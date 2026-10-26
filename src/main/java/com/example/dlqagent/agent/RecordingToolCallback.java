package com.example.dlqagent.agent;

import com.example.dlqagent.domain.ToolCallRecord;
import java.util.List;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.definition.ToolDefinition;
import org.springframework.ai.tool.metadata.ToolMetadata;

/**
 * Decorates a tool so each call is summarized into the audit trail. It records the tool name, whether it
 * succeeded and how long it took; arguments and results are never kept, because they can contain payload data.
 */
final class RecordingToolCallback implements ToolCallback {

    private final ToolCallback delegate;
    private final List<ToolCallRecord> sink;

    RecordingToolCallback(ToolCallback delegate, List<ToolCallRecord> sink) {
        this.delegate = delegate;
        this.sink = sink;
    }

    @Override
    public ToolDefinition getToolDefinition() {
        return delegate.getToolDefinition();
    }

    @Override
    public ToolMetadata getToolMetadata() {
        return delegate.getToolMetadata();
    }

    @Override
    public String call(String toolInput) {
        return record(() -> delegate.call(toolInput));
    }

    @Override
    public String call(String toolInput, ToolContext toolContext) {
        return record(() -> delegate.call(toolInput, toolContext));
    }

    private String record(java.util.function.Supplier<String> call) {
        long start = System.nanoTime();
        boolean ok = false;
        try {
            String result = call.get();
            ok = true;
            return result;
        } finally {
            sink.add(new ToolCallRecord(delegate.getToolDefinition().name(), ok, (System.nanoTime() - start) / 1_000_000));
        }
    }
}
