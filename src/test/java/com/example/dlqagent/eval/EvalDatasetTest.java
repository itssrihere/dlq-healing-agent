package com.example.dlqagent.eval;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.dlqagent.domain.Evaluation;
import com.example.dlqagent.infrastructure.DlqAgentProperties;
import com.example.dlqagent.json.StrictJson;
import com.example.dlqagent.stream.StreamRegistry;
import com.example.dlqagent.support.TestProps;
import com.example.dlqagent.support.TestStreams;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Sanity of the hand-written labels, without a model. A label that is wrong makes the evaluation measure the author,
 * not the agent, so every label that can be checked mechanically is checked here.
 */
class EvalDatasetTest {

    private static final Set<String> OUTCOMES = Set.of("REPLAYED", "HELD_FOR_APPROVAL", "QUARANTINED", "RETRY_SCHEDULED");
    private static final Set<String> STRATEGIES = Set.of("SCHEMA_DRIFT", "TRANSIENT", "POISON_PILL");

    private final List<EvalCase> cases = EvalDataset.load();
    private final List<EvalCase> casesV2 = EvalDataset.load("eval/cases-v2.jsonl");

    private static StreamRegistry streams() {
        DlqAgentProperties d = TestProps.defaults();
        var shipments = new DlqAgentProperties.StreamConfig("shipments", "shipments-dlq", "shipments-topic", null,
                new DlqAgentProperties.Contract("classpath:contracts/shipments.schema.json",
                        List.of("shipmentId", "orderRef"), "shipmentId", List.of(), List.of()), null, null, null);
        return TestStreams.registry(new DlqAgentProperties(d.topics(), d.retry(), d.planner(), d.gate(), d.approval(),
                List.of(TestProps.ordersStream(), shipments)));
    }

    @Test
    void theSetIsLargeAndEveryCaseIsWellFormed() {
        assertThat(cases).hasSizeGreaterThanOrEqualTo(100);
        assertThat(cases.stream().map(EvalCase::id).distinct()).hasSameSizeAs(cases);
        for (EvalCase c : cases) {
            assertThat(Set.of("orders", "shipments")).contains(c.stream());
            assertThat(c.expected().ideal()).as(c.id()).isNotEmpty().isSubsetOf(OUTCOMES);
            assertThat(c.expected().acceptable()).as(c.id()).isSubsetOf(OUTCOMES);
            assertThat(c.expected().strategies()).as(c.id()).isNotEmpty().isSubsetOf(STRATEGIES);
            assertThat(c.key()).as(c.id()).isNotBlank();
            assertThat(c.stackTrace()).as(c.id()).isNotBlank();
        }
    }

    @Test
    void v2IsV1WithOnlyTheMisleadingStackTraceReplacedEverywhereItAppears() {
        String misleading = "SerializationException: Can't deserialize data [[123, 34, 111]]";
        assertThat(casesV2).hasSameSizeAs(cases);
        int replaced = 0;
        for (int i = 0; i < cases.size(); i++) {
            EvalCase a = cases.get(i);
            EvalCase b = casesV2.get(i);
            // everything the labels depend on is identical: ids, payloads, headers and every expected field
            assertThat(b.id()).isEqualTo(a.id());
            assertThat(b.stream()).isEqualTo(a.stream());
            assertThat(b.category()).isEqualTo(a.category());
            assertThat(b.key()).isEqualTo(a.key());
            assertThat(b.payload()).as(a.id()).isEqualTo(a.payload());
            assertThat(b.headers()).as(a.id()).isEqualTo(a.headers());
            assertThat(b.originalTopic()).as(a.id()).isEqualTo(a.originalTopic());
            assertThat(b.expected()).as(a.id()).isEqualTo(a.expected());
            if (a.stackTrace().contains(misleading)) {
                replaced++;
                assertThat(b.stackTrace()).as(a.id()).doesNotContain("SerializationException");
            } else {
                assertThat(b.stackTrace()).as(a.id() + " must be untouched").isEqualTo(a.stackTrace());
            }
        }
        assertThat(replaced).as("the template appears in 9 v1 cases, and v2 fixes all of them, not only the ones that failed").isEqualTo(9);
    }

    @Test
    void everyMajorKindOfFailureIsRepresentedOnBothStreams() {
        Map<String, Long> byCategory = cases.stream().collect(Collectors.groupingBy(EvalCase::category, Collectors.counting()));
        assertThat(byCategory).containsKeys("mechanical_number", "mechanical_case", "mechanical_extra_field",
                "date_reformat_hold", "value_mapping_hold", "unfixable_value", "identity_problem", "transient", "poison",
                "injection", "spoofed_topic");
        for (String stream : List.of("orders", "shipments")) {
            for (String cat : List.of("mechanical_number", "transient", "poison", "date_reformat_hold")) {
                assertThat(cases.stream().filter(c -> c.stream().equals(stream) && c.category().equals(cat)))
                        .as(stream + "/" + cat).isNotEmpty();
            }
        }
    }

    @Test
    void everyAutomaticReplayLabelIsAFixTheGateWouldAcceptWithNoFlags() {
        StreamRegistry registry = streams();
        for (EvalCase c : cases) {
            boolean replayAllowed = c.expected().ideal().contains("REPLAYED") || c.expected().acceptable().contains("REPLAYED");
            if (!replayAllowed) {
                assertThat(c.expected().correctedPayload()).as(c.id() + " has a corrected payload but no replay is allowed").isNull();
                continue;
            }
            assertThat(c.expected().correctedPayload()).as(c.id()).isNotNull();
            Evaluation e = registry.byName(c.stream()).orElseThrow().adapter()
                    .evaluate(c.payload(), c.expected().correctedPayload(), c.key());
            // if the gate itself would flag or reject the label's own answer, the label is wrong or the case is not mechanical
            assertThat(e.violations()).as(c.id() + " violations").isEmpty();
            assertThat(e.riskFlags()).as(c.id() + " risk flags").isEmpty();
            assertThat(EvalScorer.sameJson(c.payload(), c.expected().correctedPayload())).as(c.id() + " changes nothing").isFalse();
        }
    }

    @Test
    void casesThatMustNotBeRepairedReallyAreBrokenPayloadsOrHaveNoMechanicalFix() {
        for (EvalCase c : cases.stream().filter(c -> c.category().equals("poison")).toList()) {
            assertThatThrownBy(() -> StrictJson.parseObject(c.payload())).as(c.id() + " must not parse as an object")
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(c.expected().ideal()).as(c.id()).containsExactly("QUARANTINED");
            assertThat(c.expected().strategies()).as(c.id()).containsExactly("POISON_PILL");
        }
        for (EvalCase c : cases.stream().filter(c -> c.category().equals("transient")).toList()) {
            // a transient case carries a valid event: nothing is wrong with the data, only the infrastructure
            var adapter = streams().byName(c.stream()).orElseThrow().adapter();
            assertThat(adapter.validate(c.payload())).as(c.id() + " payload must be valid").isEmpty();
            assertThat(c.expected().ideal()).as(c.id()).containsExactly("RETRY_SCHEDULED");
        }
    }

    @Test
    void spoofedTopicCasesNameATopicTheirStreamDoesNotServe() {
        StreamRegistry registry = streams();
        for (EvalCase c : cases.stream().filter(c -> c.category().equals("spoofed_topic")).toList()) {
            assertThat(c.originalTopic()).as(c.id()).isNotNull()
                    .isNotEqualTo(registry.byName(c.stream()).orElseThrow().sourceTopic());
            assertThat(c.expected().ideal()).as(c.id()).containsExactly("QUARANTINED");
        }
    }
}
