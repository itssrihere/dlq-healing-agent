package com.example.dlqagent.json;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.dlqagent.domain.FieldChange;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonDiffTest {

    @Test
    void reportsChangedAddedAndRemovedFieldsOnly() {
        List<FieldChange> changes = JsonDiff.diffOrEmpty(
                "{\"id\":\"1\",\"amount\":\"19.99 USD\",\"total\":5,\"same\":true}",
                "{\"id\":\"1\",\"amount\":19.99,\"currency\":\"USD\",\"same\":true}");

        assertThat(changes).containsExactlyInAnyOrder(
                new FieldChange("amount", "\"19.99 USD\"", "19.99"),
                new FieldChange("total", "5", null),
                new FieldChange("currency", null, "\"USD\""));
    }

    @Test
    void identicalPayloadsHaveNoChanges() {
        assertThat(JsonDiff.diffOrEmpty("{\"a\":1}", "{\"a\":1}")).isEmpty();
    }

    @Test
    void typeChangeIsAChange() {
        assertThat(JsonDiff.diffOrEmpty("{\"a\":\"1\"}", "{\"a\":1}")).hasSize(1);
    }

    @Test
    void unparseableInputYieldsNoDiffInsteadOfAnException() {
        assertThat(JsonDiff.diffOrEmpty("not json", "{\"a\":1}")).isEmpty();
        assertThat(JsonDiff.diffOrEmpty(null, "{\"a\":1}")).isEmpty();
        assertThat(JsonDiff.diffOrEmpty("[1]", "{\"a\":1}")).isEmpty();
    }
}
