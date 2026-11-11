package com.example.dlqagent.infrastructure;

import com.example.dlqagent.observability.LogSafe;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.InvalidMediaTypeException;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Makes "a cross-site request cannot change anything" a property of the whole service, not something each endpoint has to
 * remember. CSRF tokens are off (this is a stateless Basic-auth API for scripts), and a browser can resend cached Basic
 * credentials cross-site, so the defence has to be elsewhere. It is here, applied to every request that is not a read:
 *
 * <ul>
 *   <li>The request must carry {@code Content-Type: application/json}. A cross-site HTML form cannot send that type, and a
 *       cross-site fetch that does needs a CORS preflight, which this service never approves (it has no CORS configuration).
 *   <li>If the browser says the request came from another site ({@code Sec-Fetch-Site} is anything but {@code same-origin} or
 *       {@code none}), it is refused whatever its content type. Scripts and curl send no such header and are unaffected.
 * </ul>
 *
 * <p>It runs after authentication and authorisation, so a caller without credentials still gets a plain 401, and learns
 * nothing about these rules. A forged cross-site request arrives with the browser's cached credentials, is authenticated, and
 * is stopped here. The controllers' own {@code consumes = application/json} stays as a second layer.
 */
final class StateChangingRequestGuard extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(StateChangingRequestGuard.class);
    /** Matched exactly: a servlet container hands over the method as the client sent it, so "get" is not a read. */
    private static final Set<String> READS = Set.of("GET", "HEAD", "OPTIONS", "TRACE");
    /** The values a browser sends for its own site (and "none" for a user-typed address), exactly as the Fetch spec defines them. */
    private static final Set<String> SAME_ORIGIN = Set.of("same-origin", "none");

    /** The two ways a request is refused. Constants, so what is written to the client can never contain request data. */
    private enum Refusal {
        CROSS_SITE(HttpServletResponse.SC_FORBIDDEN, "requests from another site may not change state"),
        NOT_JSON(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE, "state-changing requests must send Content-Type: application/json");

        final int status;
        final String reason;
        final byte[] body;

        Refusal(int status, String reason) {
            this.status = status;
            this.reason = reason;
            this.body = ("{\"error\":\"" + reason + "\"}").getBytes(StandardCharsets.UTF_8);
        }
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (READS.contains(request.getMethod())) {
            chain.doFilter(request, response);
            return;
        }
        String site = request.getHeader("Sec-Fetch-Site");
        if (site != null && !SAME_ORIGIN.contains(site.trim())) {
            reject(request, response, Refusal.CROSS_SITE);
            return;
        }
        if (!isJson(request.getContentType())) {
            reject(request, response, Refusal.NOT_JSON);
            return;
        }
        chain.doFilter(request, response);
    }

    /**
     * Exactly application/json, with or without parameters such as a charset. Not application/*+json, not a wildcard. Spring's
     * parser lower-cases the type and subtype itself, so a plain comparison is enough and no further case mapping is done.
     */
    private static boolean isJson(String contentType) {
        if (contentType == null || contentType.isBlank()) {
            return false;
        }
        try {
            MediaType type = MediaType.parseMediaType(contentType);
            return "application".equals(type.getType()) && "json".equals(type.getSubtype());
        } catch (InvalidMediaTypeException unparseable) {
            return false;
        }
    }

    /** Writes the status directly: sendError would dispatch to /error, which is denied, and turn this into a 401 or 403. */
    private static void reject(HttpServletRequest request, HttpServletResponse response, Refusal refusal) throws IOException {
        log.warn("Refused {} {}: {}", LogSafe.clean(request.getMethod()), LogSafe.clean(request.getRequestURI()),
                LogSafe.clean(refusal.reason));
        response.setStatus(refusal.status);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getOutputStream().write(refusal.body);
    }
}
