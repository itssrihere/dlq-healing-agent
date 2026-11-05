package com.example.dlqagent.eval;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Aggregates scored results into the numbers and the markdown report. No model involved, so it is unit-tested. */
public final class EvalReport {

    /** One case on one run. */
    public record Result(EvalCase evalCase, int run, Observed observed, Verdict verdict) {}

    private EvalReport() {}

    public static Result score(EvalCase c, int run, Observed o) {
        return new Result(c, run, o, EvalScorer.score(c, o));
    }

    static Map<Verdict, Long> counts(List<Result> results) {
        Map<Verdict, Long> m = new EnumMap<>(Verdict.class);
        for (Verdict v : Verdict.values()) {
            m.put(v, results.stream().filter(r -> r.verdict() == v).count());
        }
        return m;
    }

    private static String pct(long part, long whole) {
        return whole == 0 ? "n/a" : "%.1f%%".formatted(100.0 * part / whole);
    }

    /** Cases whose verdict differs between runs: the model's answer is not stable for them. */
    static List<String> unstable(List<Result> results) {
        Map<String, Set<Verdict>> byCase = new LinkedHashMap<>();
        results.forEach(r -> byCase.computeIfAbsent(r.evalCase().id(), k -> new LinkedHashSet<>()).add(r.verdict()));
        return byCase.entrySet().stream().filter(e -> e.getValue().size() > 1)
                .map(e -> e.getKey() + " " + e.getValue()).toList();
    }

    static long percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) {
            return 0;
        }
        return sorted.get((int) Math.min(sorted.size() - 1, Math.ceil(p * sorted.size()) - 1));
    }

    public static String markdown(String model, int runs, List<Result> results) {
        StringBuilder md = new StringBuilder();
        long distinct = results.stream().map(r -> r.evalCase().id()).distinct().count();
        List<Result> valid = results.stream().filter(r -> r.verdict() != Verdict.ERROR).toList();
        Map<Verdict, Long> all = counts(results);
        md.append("# Labelled evaluation: ").append(model).append("\n\n")
                .append("%d distinct cases x %d runs = %d calls. Labels were written by hand; see eval/README.md.\n\n"
                        .formatted(distinct, runs, results.size()));

        md.append("## Verdicts (percentages exclude errors)\n\n| Verdict | Calls | Share |\n|---|---|---|\n");
        for (Verdict v : Verdict.values()) {
            md.append("| %s | %d | %s |\n".formatted(v, all.get(v), v == Verdict.ERROR ? "-" : pct(all.get(v), valid.size())));
        }

        long unsafe = all.get(Verdict.UNSAFE_REPLAY);
        long unsafeCases = results.stream().filter(r -> r.verdict() == Verdict.UNSAFE_REPLAY)
                .map(r -> r.evalCase().id()).distinct().count();
        md.append("\n**Unsafe replays: %d calls in %d distinct cases.** ".formatted(unsafe, unsafeCases));
        if (unsafe == 0 && distinct > 0) {
            md.append(("Zero in %d distinct cases only bounds the true rate below about %.1f%% with 95%% confidence "
                    + "(rule of three, 3/n), and the cases are hand-written and probably easier than real traffic.\n")
                            .formatted(distinct, 300.0 / distinct));
        } else {
            md.append("See the list below.\n");
        }

        List<Result> tier1 = valid.stream().filter(r -> r.evalCase().expected().ideal().contains("REPLAYED")).toList();
        md.append("\n## Automation on mechanical cases (ideal outcome: automatic replay)\n\n")
                .append("%d calls. Replayed with exactly the right payload: %s. Held or quarantined instead (safe, but no automation): %s. "
                        .formatted(tier1.size(), pct(tier1.stream().filter(r -> r.verdict() == Verdict.CORRECT).count(), tier1.size()),
                                pct(tier1.stream().filter(r -> r.verdict() == Verdict.SAFE_MISS).count(), tier1.size())))
                .append("Wrong payload replayed: %s.\n".formatted(
                        pct(tier1.stream().filter(r -> r.verdict() == Verdict.UNSAFE_REPLAY).count(), tier1.size())));

        List<Boolean> strat = valid.stream().map(r -> EvalScorer.strategyRight(r.evalCase(), r.observed()))
                .filter(java.util.Objects::nonNull).toList();
        md.append("\n## Planner classification\n\nRight strategy on %s of %d classified calls.\n"
                .formatted(pct(strat.stream().filter(b -> b).count(), strat.size()), strat.size()));

        md.append("\n## By category\n\n| Category | Calls | CORRECT | ACCEPTABLE | SAFE_MISS | WRONG_ROUTE | UNSAFE_REPLAY | ERROR | Strategy right |\n|---|---|---|---|---|---|---|---|---|\n");
        Map<String, List<Result>> byCat = results.stream().collect(Collectors.groupingBy(
                r -> r.evalCase().stream() + "/" + r.evalCase().category(), LinkedHashMap::new, Collectors.toList()));
        byCat.forEach((cat, rs) -> {
            Map<Verdict, Long> c = counts(rs);
            List<Boolean> s = rs.stream().map(r -> EvalScorer.strategyRight(r.evalCase(), r.observed()))
                    .filter(java.util.Objects::nonNull).toList();
            md.append("| %s | %d | %d | %d | %d | %d | %d | %d | %s |\n".formatted(cat, rs.size(), c.get(Verdict.CORRECT),
                    c.get(Verdict.ACCEPTABLE), c.get(Verdict.SAFE_MISS), c.get(Verdict.WRONG_ROUTE),
                    c.get(Verdict.UNSAFE_REPLAY), c.get(Verdict.ERROR), pct(s.stream().filter(b -> b).count(), s.size())));
        });

        appendList(md, "Unsafe replays", results, Verdict.UNSAFE_REPLAY);
        appendList(md, "Wrong routes", results, Verdict.WRONG_ROUTE);
        appendList(md, "Errors (model or API failed; excluded from the rates)", results, Verdict.ERROR);
        List<String> unstable = unstable(results);
        md.append("\n## Unstable cases (verdict differs between runs): %d\n\n".formatted(unstable.size()));
        unstable.forEach(u -> md.append("- ").append(u).append("\n"));

        List<Long> lat = valid.stream().map(r -> r.observed().latencyMs()).sorted().toList();
        md.append("\n## Latency and tools\n\nTriage latency p50 %d ms, p95 %d ms. Tool calls per case: %.2f on average.\n"
                .formatted(percentile(lat, 0.5), percentile(lat, 0.95),
                        valid.stream().mapToInt(r -> r.observed().toolCalls()).average().orElse(0)));
        return md.toString();
    }

    private static void appendList(StringBuilder md, String title, List<Result> results, Verdict v) {
        List<Result> rs = results.stream().filter(r -> r.verdict() == v).toList();
        md.append("\n## %s: %d\n\n".formatted(title, rs.size()));
        for (Result r : rs.stream().limit(40).toList()) {
            md.append("- `%s` run %d: outcome %s, planner %s, expected %s%s. %s\n".formatted(r.evalCase().id(), r.run(),
                    r.observed().outcome(), r.observed().plannedStrategy(), r.evalCase().expected().ideal(),
                    r.observed().finalPayload() != null && v == Verdict.UNSAFE_REPLAY ? ", payload " + r.observed().finalPayload() : "",
                    r.observed().error() != null ? r.observed().error() : trim(r.observed().reason())));
        }
    }

    private static String trim(String s) {
        return s == null ? "" : s.length() > 160 ? s.substring(0, 160) + "..." : s;
    }

    public static void writeJson(Path file, String model, int runs, List<Result> results) throws IOException {
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Result r : results) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", r.evalCase().id());
            row.put("run", r.run());
            row.put("verdict", r.verdict());
            row.put("expectedIdeal", r.evalCase().expected().ideal());
            row.put("observed", r.observed());
            rows.add(row);
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("model", model);
        doc.put("runs", runs);
        doc.put("results", rows);
        Files.writeString(file, mapper.writeValueAsString(doc));
    }
}
