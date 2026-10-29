package com.example.dlqagent;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.dlqagent.infrastructure.DlqAgentProperties;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaAdmin;

class InfrastructureSmokeIT extends AbstractIntegrationTest {

    @Autowired JdbcClient jdbc;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired DlqAgentProperties props;

    @Test
    void flywayCreatedQuarantineAndRetryTables() {
        Integer tables = jdbc.sql("""
                SELECT count(*) FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name IN ('quarantine_event', 'retry_schedule')
                """).query(Integer.class).single();

        assertThat(tables).isEqualTo(2);
    }

    @Test
    void mainAndDlqTopicsExist() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.copyOf(kafkaAdmin.getConfigurationProperties()))) {
            Set<String> topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);

            assertThat(topics).contains(props.topics().main(), props.topics().dlq());
        }
    }

    @Test
    void propertiesBindWithDefaults() {
        assertThat(props.topics().main()).isEqualTo("orders-topic");
        assertThat(props.topics().dlq()).isEqualTo("orders-dlq");
        assertThat(props.retry().maxAttempts()).isEqualTo(3);
    }
}
