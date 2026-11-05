package com.example.dlqagent.eval;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.dlqagent.AbstractIntegrationTest;
import com.example.dlqagent.agent.DlqTriageService;
import com.example.dlqagent.domain.AuditEntry;
import com.example.dlqagent.domain.FailureContext;
import com.example.dlqagent.domain.PayloadRepairer;
import com.example.dlqagent.domain.QuarantineRecord;
import com.example.dlqagent.domain.RemediationPlan;
import com.example.dlqagent.domain.RemediationPlanner;
import com.example.dlqagent.domain.RepairResult;
import com.example.dlqagent.eval.EvalReport.Result;
import com.example.dlqagent.infrastructure.DlqAgentProperties;
import com.example.dlqagent.observability.TraceContext;
import com.example.dlqagent.retry.RetryScheduler;
import com.example.dlqagent.stream.HealingStream;
import com.example.dlqagent.stream.StreamRegistry;
import io.micrometer.observation.ObservationRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The labelled evaluation: every case in eval/cases.jsonl goes through the real planner, repair agent, evaluator gate,
 * tier policy and triage service, against the real configured model. Kafka and Postgres are replaced by in-memory
 * stand-ins, so nothing is sent or stored.
 *
 * <p>Opt-in and report-only: it needs {@code DLQ_LIVE_LLM=true}, {@code DLQ_EVAL=true} and an {@code OPENAI_API_KEY}, and
 * it asserts nothing about accuracy. It writes {@code target/eval/<model>-<time>.md} and {@code .json}. The one thing it
 * does assert is that the run is valid: if more than 5% of calls hit a model or API error, the numbers mean nothing.
 *
 * <pre>
 * DLQ_LIVE_LLM=true DLQ_EVAL=true mvn verify -Dit.test=LabelledEvalIT                       # the default model, 3 runs
 * DLQ_LIVE_LLM=true DLQ_EVAL=true OPENAI_MODEL=gpt-4.1 mvn verify -Dit.test=LabelledEvalIT  # another model
 * DLQ_EVAL_RUNS=1 DLQ_EVAL_FILTER=mechanical ...                                            # a quick smoke run
 * DLQ_EVAL_DATASET=v2 ...                                                                    # the corrected inputs (default v1)
 * </pre>
 *
 * <p>Spring AI retries a failed model call up to 10 times with a delay that is multiplied by 5 each time and never capped,
 * so a few network errors can stall a run for half an hour. For an evaluation, cap it:
 * {@code SPRING_AI_RETRY_MAX_ATTEMPTS=3 SPRING_AI_RETRY_BACKOFF_INITIAL_INTERVAL=1s SPRING_AI_RETRY_BACKOFF_MULTIPLIER=2
 * SPRING_AI_RETRY_BACKOFF_MAX_INTERVAL=10s}. A call that still takes longer than {@code DLQ_EVAL_CALL_TIMEOUT_SECONDS}
 * (default 240) is scored as an ERROR and the run carries on. Parallelism defaults to 2: four concurrent calls produced
 * TLS and connection-reset errors on the machine this was written on.
 */
@ActiveProfiles("live-llm")
@EnabledIfEnvironmentVariable(named = "DLQ_LIVE_LLM", matches = "true")
@EnabledIfEnvironmentVariable(named = "DLQ_EVAL", matches = "true")
class LabelledEvalIT extends AbstractIntegrationTest {

    /** Reasons the planner and repair agent give when the model or the API failed, not when the model decided. */
    private static final List<String> ERROR_MARKERS = List.of("call failed", "returned no usable decision",
            "Planner returned no", "Planner failed", "Repairer failed", "Repair agent returned no proposal",
            "Repairer returned no result");

    @Autowired RemediationPlanner planner;
    @Autowired PayloadRepairer repairer;
    @Autowired StreamRegistry streams;
    @Autowired DlqAgentProperties props;
    @Autowired TransactionTemplate tx;
    @Autowired TraceContext trace;
    @Autowired Environment env;

    /** What the planner and repairer returned for one case, captured by the recording wrappers. */
    private static final class Capture {
        volatile RemediationPlan plan;
        volatile RepairResult repair;
    }

    private static int intEnv(String name, int fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : Integer.parseInt(v.trim());
    }

    @Test
    void evaluate() throws Exception {
        int runs = intEnv("DLQ_EVAL_RUNS", 3);
        int parallelism = intEnv("DLQ_EVAL_PARALLELISM", 2);
        int callTimeoutSeconds = intEnv("DLQ_EVAL_CALL_TIMEOUT_SECONDS", 240);
        String filter = System.getenv("DLQ_EVAL_FILTER");
        String model = env.getProperty("spring.ai.openai.chat.options.model", "unknown");
        String dataset = "v2".equalsIgnoreCase(System.getenv("DLQ_EVAL_DATASET")) ? "v2" : "v1";
        List<EvalCase> cases = EvalDataset.load("v2".equals(dataset) ? "eval/cases-v2.jsonl" : "eval/cases.jsonl").stream()
                .filter(c -> filter == null || filter.isBlank() || c.id().contains(filter) || c.category().contains(filter))
                .toList();
        assertThat(cases).as("no case matches DLQ_EVAL_FILTER").isNotEmpty();

        InMemoryStores stores = new InMemoryStores();
        Map<String, Capture> captures = new ConcurrentHashMap<>();
        RemediationPlanner recordingPlanner = ctx -> {
            RemediationPlan plan = planner.plan(ctx);
            captures.computeIfAbsent(ctx.correlationId(), k -> new Capture()).plan = plan;
            return plan;
        };
        PayloadRepairer recordingRepairer = ctx -> {
            RepairResult result = repairer.repair(ctx);
            captures.computeIfAbsent(ctx.correlationId(), k -> new Capture()).repair = result;
            return result;
        };
        // the real triage service: same tier policy, same gate, same retry scheduling; only storage and broker are fake
        DlqTriageService service = new DlqTriageService(recordingPlanner, recordingRepairer, stores.replays,
                stores.quarantine, stores.audit, ObservationRegistry.NOOP, tx, trace,
                new RetryScheduler(stores.retries, props, Clock.systemUTC()));

        System.out.printf("EVAL %s dataset %s: %d cases x %d runs, parallelism %d%n", model, dataset, cases.size(), runs, parallelism);
        ExecutorService pool = Executors.newFixedThreadPool(parallelism);
        /** A submitted (case, run) and its pending result, so a call that hangs can still be reported. */
        record Task(EvalCase evalCase, int run, Future<Result> future) {}
        List<Task> tasks = new ArrayList<>();
        AtomicInteger done = new AtomicInteger();
        int total = cases.size() * runs;
        for (int run = 0; run < runs; run++) {
            for (int i = 0; i < cases.size(); i++) {
                EvalCase c = cases.get(i);
                int r = run;
                int index = i;
                tasks.add(new Task(c, r, pool.submit(() -> {
                    Result result = EvalReport.score(c, r, observe(service, stores, captures, c, r, index));
                    int n = done.incrementAndGet();
                    if (n % 25 == 0) {
                        System.out.printf("EVAL progress %d/%d%n", n, total);
                    }
                    return result;
                })));
            }
        }
        List<Result> results = new ArrayList<>();
        for (Task t : tasks) {
            try {
                results.add(t.future().get(callTimeoutSeconds, TimeUnit.SECONDS));
            } catch (TimeoutException e) {
                // a call stuck in the client's own retry/backoff says nothing about the model: count it as an error, keep going
                t.future().cancel(true);
                results.add(EvalReport.score(t.evalCase(), t.run(), new Observed(null, "NONE", null, List.of(), null, null,
                        0, callTimeoutSeconds * 1000L, null, "timed out after " + callTimeoutSeconds + " s (client retry or hang)")));
            }
        }
        pool.shutdownNow();
        results.sort(Comparator.comparing((Result x) -> x.evalCase().id()).thenComparingInt(Result::run));

        String stamp = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(java.time.LocalDateTime.now());
        Path dir = Path.of("target", "eval");
        Files.createDirectories(dir);
        String base = model.replaceAll("[^A-Za-z0-9._-]", "_") + "-" + dataset + "-" + stamp;
        String markdown = EvalReport.markdown(model + " (dataset " + dataset + ")", runs, results);
        Files.writeString(dir.resolve(base + ".md"), markdown);
        EvalReport.writeJson(dir.resolve(base + ".json"), model, runs, results);
        System.out.println(markdown);
        System.out.println("EVAL written to " + dir.resolve(base + ".md"));

        long errors = results.stream().filter(r -> r.verdict() == Verdict.ERROR).count();
        assertThat(errors).as("model or API errors: the run is invalid if more than 5%% of calls failed (%d of %d)", errors, results.size())
                .isLessThanOrEqualTo((long) (results.size() * 0.05));
    }

    private Observed observe(DlqTriageService service, InMemoryStores stores, Map<String, Capture> captures,
                             EvalCase c, int run, int index) {
        HealingStream stream = streams.byName(c.stream()).orElseThrow();
        String correlationId = "eval-" + run + "-" + c.id();
        // partition = run and offset = case index make every (run, case) a distinct DLQ record
        FailureContext ctx = new FailureContext(c.stream(), correlationId, stream.dlqTopic(), run, index,
                c.originalTopic() != null ? c.originalTopic() : stream.sourceTopic(), c.key(), c.payload(), c.headers(),
                c.stackTrace(), Instant.now());
        long start = System.nanoTime();
        try {
            service.triage(stream, ctx);
        } catch (RuntimeException e) {
            return new Observed(null, "NONE", null, List.of(), null, null, 0, (System.nanoTime() - start) / 1_000_000,
                    null, "triage threw " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        long latency = (System.nanoTime() - start) / 1_000_000;

        Capture cap = captures.getOrDefault(correlationId, new Capture());
        Optional<AuditEntry> audit = stores.audit.findByCorrelationId(correlationId, 10).stream()
                .filter(a -> a.stage() == AuditEntry.Stage.TRIAGE).findFirst();
        String outcome = audit.map(a -> a.outcome().name()).orElse("NONE");
        String finalPayload = switch (outcome) {
            case "REPLAYED" -> stores.replays.forCorrelation(correlationId).map(r -> r.payload()).orElse(null);
            case "HELD_FOR_APPROVAL" -> stores.quarantine.findById(ctx.eventId()).map(QuarantineRecord::proposedPayload).orElse(null);
            default -> null;
        };
        String strategy = switch (cap.plan) {
            case null -> null;
            case RemediationPlan.SchemaRepair s -> "SCHEMA_DRIFT";
            case RemediationPlan.TransientRetry t -> "TRANSIENT";
            case RemediationPlan.Quarantine q -> "POISON_PILL";
        };
        return new Observed(strategy, outcome, finalPayload, audit.map(AuditEntry::riskFlags).orElse(List.of()),
                audit.map(AuditEntry::plannerConfidence).orElse(null), audit.map(AuditEntry::repairConfidence).orElse(null),
                audit.map(a -> a.toolCalls().size()).orElse(0), latency, audit.map(AuditEntry::outcomeReason).orElse(null),
                modelError(cap));
    }

    /** Non-null when the plan or repair is a fail-closed fallback caused by an error, which proves nothing about the model. */
    private static String modelError(Capture cap) {
        if (cap.plan == null) {
            return "the planner was never called";
        }
        if (cap.plan instanceof RemediationPlan.Quarantine q && mentionsError(q.reason())) {
            return q.reason();
        }
        if (cap.repair instanceof RepairResult.Unrepairable u && mentionsError(u.reason())) {
            return u.reason();
        }
        return null;
    }

    private static boolean mentionsError(String reason) {
        return reason != null && ERROR_MARKERS.stream().anyMatch(reason::contains);
    }
}
