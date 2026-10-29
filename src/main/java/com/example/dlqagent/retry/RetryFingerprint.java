package com.example.dlqagent.retry;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Identity of an event for retry accounting: SHA-256 over topic, key and payload. Strategy B never edits the
 * payload, so this is the same every time the event fails. Headers are left out on purpose: a dead-letter
 * publisher may drop or rewrite them, and a retry loop that depends on a header surviving is not bounded.
 */
final class RetryFingerprint {

    private RetryFingerprint() {}

    static String of(String topic, String key, String payload) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            // Length-prefix each part so ("ab","c") and ("a","bc") cannot collide.
            for (String part : new String[] {topic, key == null ? "\u0000null" : key, payload}) {
                byte[] bytes = part.getBytes(StandardCharsets.UTF_8);
                sha.update(Integer.toString(bytes.length).getBytes(StandardCharsets.US_ASCII));
                sha.update((byte) ':');
                sha.update(bytes);
            }
            return HexFormat.of().formatHex(sha.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java platform", e);
        }
    }
}
