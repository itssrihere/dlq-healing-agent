package com.example.dlqagent.contract;

import com.example.dlqagent.domain.ContractSource;
import com.example.dlqagent.domain.SchemaInspector;
import com.example.dlqagent.infrastructure.DlqAgentProperties;
import java.time.Clock;
import java.util.Set;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds the main topic's adapter from configuration: the schema document and the small policy. The contract is
 * checked here, so a broken one stops the service at start-up.
 */
@Configuration
class ContractConfiguration {

    @Bean
    SchemaDrivenTopicAdapter mainTopicAdapter(DlqAgentProperties props, ContractSource source,
                                              SchemaInspector inspector, Clock clock) {
        DlqAgentProperties.Contract c = props.contract();
        ContractPolicy policy = new ContractPolicy(Set.copyOf(c.identityFields()), c.keyField(),
                Set.copyOf(c.noFutureFields()));
        return new SchemaDrivenTopicAdapter(props.topics().main(),
                new SchemaContract(source.load(c.schema()), policy, clock), inspector);
    }
}
