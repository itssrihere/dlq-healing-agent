package com.example.dlqagent.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.example.dlqagent.domain.TopicAdapter;
import com.example.dlqagent.domain.TopicAdapter.FieldType;
import com.example.dlqagent.json.StrictJson;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Engine-only: the applier is tested against a mocked adapter, with no knowledge of orders. */
class PatchApplierTest {

    private final TopicAdapter adapter = mock(TopicAdapter.class);
    private final PatchApplier applier = new PatchApplier();
    private final ObjectNode original = StrictJson.parseObject("""
            {"orderId":"o-1","customerId":"c-1","amount":"19.99 USD","total":1,"status":"PAID"}""");

    PatchApplierTest() {
        when(adapter.editableFields()).thenReturn(Map.of(
                "amount", FieldType.DECIMAL, "currency", FieldType.TEXT, "status", FieldType.TEXT));
        when(adapter.immutableFields()).thenReturn(Set.of("orderId", "customerId"));
    }

    private static PatchProposal proposal(List<FieldEdit> edits, List<String> drop) {
        return new PatchProposal(true, 0.9, "s", edits, drop);
    }

    @Test
    void appliesEditsAndDropsWithoutMutatingTheOriginal() {
        ObjectNode patched = applier.apply(adapter, original, proposal(
                List.of(new FieldEdit("amount", "19.99"), new FieldEdit("currency", "USD")), List.of("total")));

        assertThat(patched.get("amount").decimalValue()).isEqualByComparingTo("19.99");
        assertThat(patched.get("currency").asText()).isEqualTo("USD");
        assertThat(patched.has("total")).isFalse();
        assertThat(original.get("amount").isTextual()).isTrue();
        assertThat(original.has("total")).isTrue();
    }

    @Test
    void decimalIsWrittenAsPlainNumber() throws Exception {
        ObjectNode patched = applier.apply(adapter, original, proposal(List.of(new FieldEdit("amount", "1000")), List.of()));

        assertThat(StrictJson.write(patched)).contains("\"amount\":1000").doesNotContain("E+");
    }

    @Test
    void fieldsOutsideTheAdapterWhitelistCannotBeEdited() {
        for (String field : List.of("orderId", "customerId", "createdAt", "nonsense", "AMOUNT")) {
            assertThatThrownBy(() -> applier.apply(adapter, original, proposal(List.of(new FieldEdit(field, "x")), List.of())))
                    .as(field).isInstanceOf(PatchRejectedException.class).hasMessageContaining("not editable");
        }
    }

    @Test
    void identityFieldsCannotBeDropped() {
        assertThatThrownBy(() -> applier.apply(adapter, original, proposal(List.of(), List.of("orderId"))))
                .isInstanceOf(PatchRejectedException.class).hasMessageContaining("identity");
    }

    @Test
    void droppingAFieldThatIsNotThereIsRejected() {
        assertThatThrownBy(() -> applier.apply(adapter, original, proposal(List.of(), List.of("ghost"))))
                .isInstanceOf(PatchRejectedException.class);
    }

    @Test
    void nonNumericDecimalIsRejected() {
        assertThatThrownBy(() -> applier.apply(adapter, original,
                proposal(List.of(new FieldEdit("amount", "19.99 USD")), List.of())))
                .isInstanceOf(PatchRejectedException.class).hasMessageContaining("plain number");
    }

    @Test
    void duplicateMalformedAndOversizedEditsAreRejected() {
        var twice = List.of(new FieldEdit("status", "NEW"), new FieldEdit("status", "PAID"));
        assertThatThrownBy(() -> applier.apply(adapter, original, proposal(twice, List.of())))
                .isInstanceOf(PatchRejectedException.class).hasMessageContaining("duplicate");

        var nulls = List.of(new FieldEdit(null, "x"));
        assertThatThrownBy(() -> applier.apply(adapter, original, proposal(nulls, List.of())))
                .isInstanceOf(PatchRejectedException.class);

        var longValue = List.of(new FieldEdit("status", "A".repeat(65)));
        assertThatThrownBy(() -> applier.apply(adapter, original, proposal(longValue, List.of())))
                .isInstanceOf(PatchRejectedException.class);

        var many = java.util.Collections.nCopies(9, new FieldEdit("status", "NEW"));
        assertThatThrownBy(() -> applier.apply(adapter, original, proposal(many, List.of())))
                .isInstanceOf(PatchRejectedException.class).hasMessageContaining("too many");
    }
}
