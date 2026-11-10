package com.example.dlqagent.eval;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

/**
 * Turns "the labels were frozen before any model ran" from a promise into a check. Every published evaluation number is
 * only meaningful against exactly these files, so if one changes the test fails and says what to do instead.
 *
 * <p>The rule: never edit a published dataset. If a label or an input turns out to be wrong, copy the file to a new version
 * (cases-v3.jsonl), fix it there, run the models on it, and report the old and the new numbers side by side, as v2 did for
 * v1. Add the new version to {@link #FROZEN} in the same commit, with the reason in the commit message.
 */
class EvalFreezeTest {

    /** SHA-256 of each published dataset file, taken when its results were published. */
    private static final Map<String, String> FROZEN = Map.of(
            "eval/cases.jsonl", "3f44233221f1e481a0b50cd7145ef6b3771576465c42d8cd2b7a635b9dffa97d",
            "eval/cases-v2.jsonl", "ea53336f92f39e20be37454a60748a16876e8c62eaca9280827c55cbf5b59e2f");

    private static String sha256(String resource) throws IOException, NoSuchAlgorithmException {
        try (InputStream in = new ClassPathResource(resource).getInputStream()) {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(in.readAllBytes()));
        }
    }

    @Test
    void publishedDatasetsAreByteForByteWhatTheResultsWereMeasuredOn() throws Exception {
        for (var frozen : FROZEN.entrySet()) {
            assertThat(sha256(frozen.getKey()))
                    .as("%s was edited. Published evaluation results are only valid for the exact file they were measured on. "
                            + "Do not edit it: copy it to a new version (for example cases-v3.jsonl), fix it there, run the "
                            + "models on it and report both sets of numbers. Then add the new file to FROZEN in this test.",
                            frozen.getKey())
                    .isEqualTo(frozen.getValue());
        }
    }

    @Test
    void everyDatasetOnTheClasspathIsRegisteredHereSoANewOneCannotSkipTheRule() throws Exception {
        var resolver = new org.springframework.core.io.support.PathMatchingResourcePatternResolver();
        for (var resource : resolver.getResources("classpath*:eval/cases*.jsonl")) {
            assertThat(FROZEN).as("%s is not registered in EvalFreezeTest.FROZEN: freeze it with its results", resource.getFilename())
                    .containsKey("eval/" + resource.getFilename());
        }
    }
}
