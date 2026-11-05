package com.example.dlqagent.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.ai.converter.BeanOutputConverter;

class LenientOutputConverterTest {

    /** A key repeated after the last field: the shape that made 2 of about 200 gpt-4.1 repair calls fail in the evaluation. */
    private static final String TRAILING_DUPLICATE =
            "{\"repairable\":true,\"confidence\":0.9,\"summary\":\"s\",\"edits\":[],\"dropFields\":[],\"confidence\":0.7}";
    private static final String NESTED_DUPLICATE =
            "{\"repairable\":true,\"confidence\":0.9,\"summary\":\"s\",\"edits\":[{\"field\":\"a\",\"newValue\":\"1\",\"field\":\"b\"}],\"dropFields\":[]}";

    private final LenientOutputConverter<PatchProposal> lenient = new LenientOutputConverter<>(PatchProposal.class);

    @Test
    void theStockConverterFailsOnARepeatedKey() {
        // the control: without this the tests below would pass whether or not the fix did anything
        var stock = new BeanOutputConverter<>(PatchProposal.class);

        assertThatThrownBy(() -> stock.convert(TRAILING_DUPLICATE)).hasStackTraceContaining("No fallback setter/field defined for creator property");
        assertThatThrownBy(() -> stock.convert(NESTED_DUPLICATE)).hasStackTraceContaining("No fallback setter/field defined for creator property");
    }

    @Test
    void aRepeatedKeyKeepsTheLastValue() {
        PatchProposal p = lenient.convert(TRAILING_DUPLICATE);

        assertThat(p.repairable()).isTrue();
        assertThat(p.confidence()).isEqualTo(0.7);
        assertThat(lenient.convert(NESTED_DUPLICATE).edits()).singleElement().satisfies(e -> {
            assertThat(e.field()).isEqualTo("b");
            assertThat(e.newValue()).isEqualTo("1");
        });
    }

    @Test
    void wellFormedOutputBehavesExactlyAsBefore() {
        PatchProposal p = lenient.convert("{\"repairable\":true,\"confidence\":0.95,\"summary\":\"fix\",\"edits\":"
                + "[{\"field\":\"amount\",\"newValue\":\"19.99\"}],\"dropFields\":[\"debug\"]}");

        assertThat(p.confidence()).isEqualTo(0.95);
        assertThat(p.edits()).hasSize(1);
        assertThat(p.dropFields()).containsExactly("debug");
    }

    @Test
    void markdownFencesAroundTheJsonAreStillAccepted() {
        assertThat(lenient.convert("```json\n" + TRAILING_DUPLICATE + "\n```").confidence()).isEqualTo(0.7);
    }

    @Test
    void textThatIsNotAJsonObjectStillFailsWithTheOriginalError() {
        assertThatThrownBy(() -> lenient.convert("I fixed it!")).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> lenient.convert("[1,2,3]")).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> lenient.convert("{\"repairable\": ")).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> lenient.convert("")).isInstanceOf(RuntimeException.class);
    }

    @Test
    void thePlannerDecisionGetsTheSameTolerance() {
        var planner = new LenientOutputConverter<>(PlannerDecision.class);

        PlannerDecision d = planner.convert("{\"strategy\":\"TRANSIENT\",\"confidence\":0.8,\"rationale\":\"timeout\",\"strategy\":\"POISON_PILL\"}");
        assertThat(d.strategy().name()).isEqualTo("POISON_PILL"); // the last value wins, and the gate downstream judges it as usual
    }
}
