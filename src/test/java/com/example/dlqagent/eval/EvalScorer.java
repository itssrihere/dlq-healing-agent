package com.example.dlqagent.eval;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Iterator;
import java.util.Map;

/** Pure scoring logic, unit-tested without a model so the metrics themselves cannot hide a bug. */
public final class EvalScorer {

    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private EvalScorer() {}

    public static Verdict score(EvalCase c, Observed o) {
        if (o.error() != null) {
            return Verdict.ERROR;
        }
        String outcome = o.outcome();
        boolean ideal = c.expected().ideal().contains(outcome);
        boolean accepted = ideal || c.expected().acceptable().contains(outcome);
        if ("REPLAYED".equals(outcome)) {
            // an automatic replay is only ever right if it was expected AND carries exactly the corrected payload
            boolean payloadRight = c.expected().correctedPayload() != null && o.finalPayload() != null
                    && sameJson(o.finalPayload(), c.expected().correctedPayload());
            if (!accepted || !payloadRight) {
                return Verdict.UNSAFE_REPLAY;
            }
            return ideal ? Verdict.CORRECT : Verdict.ACCEPTABLE;
        }
        if (ideal) {
            return Verdict.CORRECT;
        }
        if (accepted) {
            return Verdict.ACCEPTABLE;
        }
        return "RETRY_SCHEDULED".equals(outcome) ? Verdict.WRONG_ROUTE : Verdict.SAFE_MISS;
    }

    /** Did the planner pick a classification the label allows? Null for an error. */
    public static Boolean strategyRight(EvalCase c, Observed o) {
        return o.error() != null || o.plannedStrategy() == null ? null : c.expected().strategies().contains(o.plannedStrategy());
    }

    /** JSON equality that compares numbers by value (19.99 equals 19.990) and ignores key order. */
    static boolean sameJson(String a, String b) {
        try {
            return same(JSON.readTree(a), JSON.readTree(b));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            return false;
        }
    }

    private static boolean same(JsonNode a, JsonNode b) {
        if (a.isNumber() && b.isNumber()) {
            return a.decimalValue().compareTo(b.decimalValue()) == 0;
        }
        if (a.isObject() && b.isObject()) {
            if (a.size() != b.size()) {
                return false;
            }
            Iterator<Map.Entry<String, JsonNode>> fields = a.fields();
            while (fields.hasNext()) {
                var f = fields.next();
                if (!b.has(f.getKey()) || !same(f.getValue(), b.get(f.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        if (a.isArray() && b.isArray()) {
            if (a.size() != b.size()) {
                return false;
            }
            for (int i = 0; i < a.size(); i++) {
                if (!same(a.get(i), b.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return a.equals(b);
    }
}
