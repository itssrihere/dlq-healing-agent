package com.example.dlqagent.eval;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.core.io.ClassPathResource;

/** Loads the labelled cases from {@code eval/cases.jsonl}: one JSON object per line. */
public final class EvalDataset {

    private static final ObjectMapper JSON = new ObjectMapper();

    private EvalDataset() {}

    public static List<EvalCase> load() {
        try (InputStream in = new ClassPathResource("eval/cases.jsonl").getInputStream()) {
            List<EvalCase> cases = new ArrayList<>();
            for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                if (!line.isBlank()) {
                    cases.add(parse(JSON.readTree(line)));
                }
            }
            return cases;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read eval/cases.jsonl", e);
        }
    }

    private static EvalCase parse(JsonNode n) {
        // a case carries either payloadRaw (any text, including broken JSON) or payloadJson (an object, written compactly)
        String payload = n.has("payloadRaw") ? n.get("payloadRaw").asText() : n.get("payloadJson").toString();
        JsonNode e = n.get("expected");
        Map<String, String> headers = new LinkedHashMap<>();
        if (n.has("headers")) {
            n.get("headers").fields().forEachRemaining(h -> headers.put(h.getKey(), h.getValue().asText()));
        }
        return new EvalCase(n.get("id").asText(), n.get("stream").asText(), n.get("category").asText(),
                n.get("key").asText(), payload, headers, n.get("stackTrace").asText(),
                n.has("originalTopic") ? n.get("originalTopic").asText() : null,
                new EvalCase.Expected(strings(e.get("strategies")), strings(e.get("ideal")),
                        e.has("acceptable") ? strings(e.get("acceptable")) : List.of(),
                        e.has("correctedPayload") ? e.get("correctedPayload").toString() : null),
                n.has("note") ? n.get("note").asText() : "");
    }

    private static List<String> strings(JsonNode array) {
        List<String> out = new ArrayList<>();
        array.forEach(x -> out.add(x.asText()));
        return out;
    }
}
