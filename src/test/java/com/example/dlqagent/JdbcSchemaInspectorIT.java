package com.example.dlqagent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.dlqagent.domain.SchemaInspector;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class JdbcSchemaInspectorIT extends AbstractIntegrationTest {

    @Autowired SchemaInspector inspector;

    @Test
    void describesOrdersColumnsAndConstraintsFromTheRealCatalog() {
        var schema = inspector.describe("orders");

        assertThat(schema.columns()).extracting(SchemaInspector.Column::name)
                .containsExactly("order_id", "customer_id", "amount", "currency", "status", "created_at");
        var amount = schema.columns().get(2);
        assertThat(amount.type()).isEqualTo("numeric");
        assertThat(amount.numericPrecision()).isEqualTo(12);
        assertThat(amount.numericScale()).isEqualTo(2);
        assertThat(schema.constraints()).anyMatch(c -> c.contains("PRIMARY KEY"))
                .anyMatch(c -> c.contains("amount > (0)::numeric"))
                .anyMatch(c -> c.contains("SHIPPED"));
    }

    @Test
    void tableNameIsCaseInsensitive() {
        assertThat(inspector.describe(" ORDERS ").table()).isEqualTo("orders");
    }

    @Test
    void tablesOutsideTheAllowListAreRefused() {
        assertThatThrownBy(() -> inspector.describe("quarantine_event")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> inspector.describe("orders; DROP TABLE orders")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> inspector.describe("pg_catalog.pg_user")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> inspector.describe(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
