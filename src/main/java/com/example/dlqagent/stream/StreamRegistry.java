package com.example.dlqagent.stream;

import com.example.dlqagent.contract.ContractPolicy;
import com.example.dlqagent.contract.SchemaContract;
import com.example.dlqagent.contract.SchemaDrivenTopicAdapter;
import com.example.dlqagent.domain.ContractSource;
import com.example.dlqagent.domain.SchemaInspector;
import com.example.dlqagent.infrastructure.DlqAgentProperties;
import com.example.dlqagent.infrastructure.DlqAgentProperties.StreamConfig;
import com.example.dlqagent.retry.ReplayCircuitBreaker;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.stereotype.Component;

/**
 * Builds every configured stream at start-up. A stream whose contract does not load, or whose policy disagrees with
 * its schema, stops the service with a message naming it: running without one silently would leave its DLQ unattended
 * and nobody would notice.
 */
@Component
public final class StreamRegistry {

    private final Map<String, HealingStream> byName = new LinkedHashMap<>();
    private final Map<String, HealingStream> byDlqTopic = new LinkedHashMap<>();

    public StreamRegistry(DlqAgentProperties props, ContractSource source, SchemaInspector inspector, Clock clock) {
        for (StreamConfig c : props.streams()) {
            HealingStream stream;
            try {
                stream = build(c, props, source, inspector, clock);
            } catch (RuntimeException e) {
                throw new IllegalStateException("Stream '" + c.name() + "' cannot start: " + e.getMessage(), e);
            }
            byName.put(stream.name(), stream);
            byDlqTopic.put(stream.dlqTopic(), stream);
        }
    }

    private static HealingStream build(StreamConfig c, DlqAgentProperties props, ContractSource source,
                                       SchemaInspector inspector, Clock clock) {
        DlqAgentProperties.Contract contract = c.contract();
        ContractPolicy policy = new ContractPolicy(Set.copyOf(contract.identityFields()), contract.keyField(),
                Set.copyOf(contract.noFutureFields()));
        SchemaDrivenTopicAdapter adapter = new SchemaDrivenTopicAdapter(c.sourceTopic(),
                new SchemaContract(source.load(contract.schema()), policy, clock), inspector,
                Set.copyOf(contract.tables()));
        double threshold = c.autoReplayMinConfidence() != null ? c.autoReplayMinConfidence()
                : props.gate().autoReplayMinConfidence();
        int attempts = c.maxAttempts() != null ? c.maxAttempts() : props.retry().maxAttempts();
        return new HealingStream(c.name(), c.dlqTopic(), c.sourceTopic(), c.groupIdOrDefault(), adapter,
                c.autoReplayEnabled(), threshold, attempts, new ReplayCircuitBreaker(clock, props.retry().breaker()));
    }

    public List<HealingStream> all() {
        return new ArrayList<>(byName.values());
    }

    public Optional<HealingStream> byName(String name) {
        return Optional.ofNullable(byName.get(name));
    }

    public Optional<HealingStream> forDlqTopic(String dlqTopic) {
        return Optional.ofNullable(byDlqTopic.get(dlqTopic));
    }
}
