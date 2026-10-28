package com.example.dlqagent.support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads container image names from {@code docker-compose.yml}, so the integration tests run against exactly the same
 * Kafka and PostgreSQL versions as local development. There is one place to change a version, and when Dependabot
 * bumps an image in the compose file its pull request runs the whole test suite against the new version (it used to
 * bump only the compose file, which no test exercised).
 */
public final class ComposeImages {

    private static final Pattern IMAGE_LINE = Pattern.compile("^\\s*image:\\s*(\\S+)\\s*(#.*)?$");

    private ComposeImages() {}

    /** @return the full image reference, for example {@code apache/kafka:4.3.1}, for the given repository name */
    public static String of(String repository) {
        return of(repository, Path.of("docker-compose.yml"));
    }

    static String of(String repository, Path composeFile) {
        try {
            for (String line : Files.readAllLines(composeFile)) {
                Matcher m = IMAGE_LINE.matcher(line);
                if (m.matches() && m.group(1).startsWith(repository + ":")) {
                    return m.group(1);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read " + composeFile.toAbsolutePath()
                    + " (tests must run from the project root)", e);
        }
        throw new IllegalStateException("No 'image: " + repository + ":<tag>' line in " + composeFile.toAbsolutePath());
    }
}
