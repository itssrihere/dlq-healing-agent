package com.example.dlqagent.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.example.dlqagent.domain.ToolCallRecord;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.definition.ToolDefinition;

class RecordingToolCallbackTest {

    private final ToolCallback delegate = mock(ToolCallback.class);
    private final List<ToolCallRecord> sink = new ArrayList<>();
    private final RecordingToolCallback recording = new RecordingToolCallback(delegate, sink);

    RecordingToolCallbackTest() {
        ToolDefinition def = ToolDefinition.builder().name("describeTable").description("d").inputSchema("{}").build();
        when(delegate.getToolDefinition()).thenReturn(def);
    }

    @Test
    void recordsTheNameAndSuccessOfACallAndReturnsTheResultUnchanged() {
        when(delegate.call("{\"tableName\":\"orders\"}")).thenReturn("the schema");

        assertThat(recording.call("{\"tableName\":\"orders\"}")).isEqualTo("the schema");

        assertThat(sink).hasSize(1);
        assertThat(sink.get(0).tool()).isEqualTo("describeTable");
        assertThat(sink.get(0).succeeded()).isTrue();
        assertThat(sink.get(0).durationMs()).isNotNegative();
    }

    @Test
    void recordsAFailedCallAndRethrowsTheOriginalException() {
        when(delegate.call("bad")).thenThrow(new IllegalArgumentException("Table not allowed"));

        assertThatThrownBy(() -> recording.call("bad")).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Table not allowed");

        assertThat(sink).extracting(ToolCallRecord::succeeded).containsExactly(false);
    }

    @Test
    void theContextAwareOverloadIsRecordedToo() {
        ToolContext ctx = new ToolContext(java.util.Map.of());
        when(delegate.call("x", ctx)).thenReturn("ok");

        assertThat(recording.call("x", ctx)).isEqualTo("ok");

        assertThat(sink).hasSize(1);
    }

    @Test
    void neverRecordsArgumentsOrResultsBecauseTheyCanHoldPayloadData() {
        when(delegate.call("{\"json\":\"{\\\"customerId\\\":\\\"secret-123\\\"}\"}")).thenReturn("secret-123 is valid");

        recording.call("{\"json\":\"{\\\"customerId\\\":\\\"secret-123\\\"}\"}");

        assertThat(sink.toString()).doesNotContain("secret-123");
        assertThat(ToolCallRecord.class.getRecordComponents()).extracting("name")
                .containsExactly("tool", "succeeded", "durationMs");
    }
}
