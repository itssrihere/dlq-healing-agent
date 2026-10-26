package com.example.dlqagent.observability;

/**
 * Makes untrusted text safe to put in a log line. Payloads, headers, exception messages and model output can all
 * contain line breaks, so an attacker could otherwise forge extra log entries (log injection).
 */
public final class LogSafe {

    private static final int MAX_CHARS = 500;

    private LogSafe() {}

    public static String clean(Object value) {
        if (value == null) {
            return "null";
        }
        String s = value.toString();
        StringBuilder out = new StringBuilder(Math.min(s.length(), MAX_CHARS) + 1);
        for (int i = 0; i < s.length() && i < MAX_CHARS; i++) {
            char c = s.charAt(i);
            out.append(Character.isISOControl(c) || c == '\u2028' || c == '\u2029' ? '_' : c);
        }
        if (s.length() > MAX_CHARS) {
            out.append('…');
        }
        return out.toString();
    }
}
