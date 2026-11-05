package com.example.dlqagent.eval;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.dlqagent.eval.EvalReport.Result;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EvalReportTest {

    private static EvalCase aCase(String id, String category, List<String> ideal, String corrected) {
        return new EvalCase(id, "orders", category, "k", "{}", Map.of(), "t", null,
                new EvalCase.Expected(List.of("SCHEMA_DRIFT"), ideal, List.of(), corrected), "");
    }

    private static Observed seen(String outcome, String payload, long latency) {
        return new Observed("SCHEMA_DRIFT", outcome, payload, List.of(), 0.9, 0.9, 1, latency, "why", null);
    }

    @Test
    void countsEveryVerdictAndExcludesErrorsFromTheShares() {
        EvalCase mech = aCase("m1", "mechanical_number", List.of("REPLAYED"), "{\"a\":1}");
        List<Result> rs = new ArrayList<>();
        rs.add(EvalReport.score(mech, 0, seen("REPLAYED", "{\"a\":1}", 10)));
        rs.add(EvalReport.score(mech, 1, seen("QUARANTINED", null, 20)));
        rs.add(EvalReport.score(mech, 2, new Observed(null, "QUARANTINED", null, List.of(), null, null, 0, 1, "r", "API down")));

        Map<Verdict, Long> counts = EvalReport.counts(rs);
        String md = EvalReport.markdown("m", 3, rs);

        assertThat(counts.get(Verdict.CORRECT)).isEqualTo(1);
        assertThat(counts.get(Verdict.SAFE_MISS)).isEqualTo(1);
        assertThat(counts.get(Verdict.ERROR)).isEqualTo(1);
        assertThat(md).contains("| CORRECT | 1 | 50.0% |").contains("| SAFE_MISS | 1 | 50.0% |").contains("| ERROR | 1 | - |");
    }

    @Test
    void zeroUnsafeReplaysIsReportedWithItsStatisticalLimit() {
        List<Result> rs = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            rs.add(EvalReport.score(aCase("h" + i, "date_reformat_hold", List.of("QUARANTINED"), null), 0, seen("QUARANTINED", null, 5)));
        }

        String md = EvalReport.markdown("m", 1, rs);

        assertThat(md).contains("Unsafe replays: 0 calls in 0 distinct cases").contains("below about 10.0%");
    }

    @Test
    void anUnsafeReplayIsListedWithTheWrongPayload() {
        EvalCase mech = aCase("m1", "mechanical_number", List.of("REPLAYED"), "{\"a\":1}");
        List<Result> rs = List.of(EvalReport.score(mech, 0, seen("REPLAYED", "{\"a\":2}", 5)));

        String md = EvalReport.markdown("m", 1, rs);

        assertThat(md).contains("Unsafe replays: 1 calls in 1 distinct cases").contains("`m1` run 0").contains("{\"a\":2}");
    }

    @Test
    void casesWhoseVerdictChangesBetweenRunsAreReportedAsUnstable() {
        EvalCase mech = aCase("m1", "mechanical_number", List.of("REPLAYED"), "{\"a\":1}");
        EvalCase steady = aCase("s1", "poison", List.of("QUARANTINED"), null);
        List<Result> rs = List.of(
                EvalReport.score(mech, 0, seen("REPLAYED", "{\"a\":1}", 5)),
                EvalReport.score(mech, 1, seen("QUARANTINED", null, 5)),
                EvalReport.score(steady, 0, seen("QUARANTINED", null, 5)),
                EvalReport.score(steady, 1, seen("QUARANTINED", null, 5)));

        assertThat(EvalReport.unstable(rs)).hasSize(1).first().asString().startsWith("m1");
    }

    @Test
    void percentilesPickTheRightObservation() {
        List<Long> sorted = List.of(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L, 100L);

        assertThat(EvalReport.percentile(sorted, 0.5)).isEqualTo(50);
        assertThat(EvalReport.percentile(sorted, 0.95)).isEqualTo(100);
        assertThat(EvalReport.percentile(List.of(), 0.5)).isZero();
    }
}
