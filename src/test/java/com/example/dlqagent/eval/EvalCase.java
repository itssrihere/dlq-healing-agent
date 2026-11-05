package com.example.dlqagent.eval;

import java.util.List;
import java.util.Map;

/**
 * One labelled failure: what the agent is shown, and what the right answer is. The labels are written by hand, so they
 * are a judgement, not ground truth; see src/test/resources/eval/README.md.
 *
 * @param key     the Kafka key of the failed event
 * @param payload the raw payload text, which is deliberately not always valid JSON
 */
public record EvalCase(
        String id,
        String stream,
        String category,
        String key,
        String payload,
        Map<String, String> headers,
        String stackTrace,
        String originalTopic,
        Expected expected,
        String note) {

    /**
     * @param strategies       planner classifications that count as right
     * @param ideal            outcomes that count as fully correct (any one of them)
     * @param acceptable       other safe outcomes that are tolerated but not ideal
     * @param correctedPayload the payload a correct automatic replay must carry, or null if none may be replayed
     */
    public record Expected(List<String> strategies, List<String> ideal, List<String> acceptable, String correctedPayload) {

        public Expected {
            strategies = List.copyOf(strategies);
            ideal = List.copyOf(ideal);
            acceptable = List.copyOf(acceptable);
        }
    }

    public EvalCase {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
