package com.example.dlqagent.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ComposeImagesTest {

    @TempDir Path dir;

    private Path compose(String content) throws Exception {
        return Files.writeString(dir.resolve("docker-compose.yml"), content);
    }

    @Test
    void findsTheImageForARepositoryAndIgnoresTrailingComments() throws Exception {
        Path f = compose("""
                services:
                  db:
                    image: postgres:16-alpine
                  kafka:
                    image: apache/kafka:4.3.1   # pinned
                  ui:
                    image: jaegertracing/all-in-one:1.76.0
                """);

        assertThat(ComposeImages.of("postgres", f)).isEqualTo("postgres:16-alpine");
        assertThat(ComposeImages.of("apache/kafka", f)).isEqualTo("apache/kafka:4.3.1");
    }

    @Test
    void aRepositoryNameIsNotMatchedAsAPrefixOfAnotherOne() throws Exception {
        Path f = compose("services:\n  x:\n    image: postgres-exporter:1\n");

        assertThatThrownBy(() -> ComposeImages.of("postgres", f)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void failsClearlyWhenTheImageOrTheFileIsMissing() throws Exception {
        assertThatThrownBy(() -> ComposeImages.of("apache/kafka", compose("services: {}\n")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("apache/kafka");
        assertThatThrownBy(() -> ComposeImages.of("x", dir.resolve("missing.yml")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("project root");
    }

    @Test
    void theRealComposeFileProvidesBothImagesTheIntegrationTestsNeed() {
        assertThat(ComposeImages.of("postgres")).startsWith("postgres:");
        assertThat(ComposeImages.of("apache/kafka")).startsWith("apache/kafka:");
    }
}
