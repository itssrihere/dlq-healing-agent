package com.example.dlqagent.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.dlqagent.contract.ContractTools;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Records that hold collections must copy them, so a caller keeping a reference cannot change audit-relevant data. */
class ImmutabilityTest {

    @Test
    void quarantineRecordCopiesItsHeadersAndFlags() {
        Map<String, String> headers = new HashMap<>(Map.of("a", "1"));
        List<String> flags = new ArrayList<>(List.of("f"));
        var r = new QuarantineRecord(UUID.randomUUID(), "orders", "c", "t", "k", "p", headers, null, RemediationStrategy.POISON_PILL,
                "r", QuarantineStatus.PENDING_REVIEW, Instant.now(), null, null, flags, null, null, null);

        headers.put("b", "2");
        flags.add("g");

        assertThat(r.headers()).containsOnlyKeys("a");
        assertThat(r.riskFlags()).containsExactly("f");
        assertThatThrownBy(() -> r.headers().put("c", "3")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void schemaAndValidationReportsCopyTheirLists() {
        List<String> constraints = new ArrayList<>(List.of("PRIMARY KEY"));
        var schema = new SchemaInspector.TableSchema("orders", new ArrayList<>(), constraints);
        constraints.add("CHECK");
        List<String> violations = new ArrayList<>(List.of("v"));
        var report = new ContractTools.ValidationReport(false, violations);
        violations.clear();

        assertThat(schema.constraints()).containsExactly("PRIMARY KEY");
        assertThat(report.violations()).containsExactly("v");
    }
}
