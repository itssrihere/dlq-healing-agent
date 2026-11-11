package com.example.dlqagent.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.servlet.http.HttpServletResponse;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

/** The rule on its own, without Spring: which requests the guard lets through, and which it stops before any controller. */
class StateChangingRequestGuardTest {

    private final StateChangingRequestGuard guard = new StateChangingRequestGuard();

    private record Outcome(int status, boolean reached, String body) {}

    private Outcome run(String method, String contentType, String secFetchSite) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest(method, "/api/approvals/x/approve");
        if (contentType != null) {
            request.setContentType(contentType);
        }
        if (secFetchSite != null) {
            request.addHeader("Sec-Fetch-Site", secFetchSite);
        }
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicInteger reached = new AtomicInteger();
        guard.doFilter(request, response, new MockFilterChain() {
            @Override
            public void doFilter(jakarta.servlet.ServletRequest req, jakarta.servlet.ServletResponse res) {
                reached.incrementAndGet();
            }
        });
        return new Outcome(response.getStatus(), reached.get() == 1, response.getContentAsString());
    }

    @Test
    void readsAreNeverBlockedWhateverTheirHeaders() throws Exception {
        for (String method : new String[] {"GET", "HEAD", "OPTIONS", "TRACE"}) {
            assertThat(run(method, null, null).reached()).as(method).isTrue();
            assertThat(run(method, "text/plain", "cross-site").reached()).as(method + " cross-site").isTrue();
        }
    }

    @Test
    void anUnusualOrLowerCaseMethodIsNotARead_SoItFailsClosed() throws Exception {
        for (String method : new String[] {"get", "Get", "head", "options", "CUSTOM", "PROPFIND"}) {
            assertThat(run(method, "text/plain", null).status()).as(method).isEqualTo(415);
            assertThat(run(method, "text/plain", null).reached()).as(method).isFalse();
        }
    }

    @Test
    void aStateChangingRequestMustBeJsonWithOrWithoutParameters() throws Exception {
        for (String json : new String[] {"application/json", "application/json;charset=UTF-8", "Application/JSON", " application/json "}) {
            assertThat(run("POST", json, null).reached()).as(json).isTrue();
        }
    }

    @Test
    void everyOtherContentTypeIsRefusedWith415BeforeTheControllerRuns() throws Exception {
        // the types a cross-site form or a no-cors fetch can send, and the near misses a sloppy check would let through
        String[] refused = {null, "", "  ", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x", "text/plain",
                "text/json", "application/jsonp", "application/x-json", "application/merge-patch+json", "application/*",
                "*/*", "application/", "///", "not a media type", "application/json, text/plain"};
        for (String type : refused) {
            Outcome o = run("POST", type, null);
            assertThat(o.status()).as("content type [" + type + "]").isEqualTo(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);
            assertThat(o.reached()).as("content type [" + type + "] must not reach the controller").isFalse();
        }
    }

    @Test
    void unicodeLookAlikesOfJsonAreNotJson() throws Exception {
        // fullwidth letters, a dotless i, a long s, a look-alike n and a zero-width space: none of them is application/json.
        // Spring's strict media-type parser already rejects non-ASCII tokens, so this pins the end-to-end behaviour; the guard's own
        // exact comparison is a second layer that this input cannot distinguish from a case-insensitive one.
        // (A NUL or other control character cannot reach the guard: a real servlet container rejects it in a header.)
        for (String lookAlike : new String[] {"\uFF41pplication/json", "applicat\u0131on/json", "application/j\u017Fon",
                "application/jso\u0578", "application\u200B/json"}) {
            assertThat(run("POST", lookAlike, null).status()).as(lookAlike).isEqualTo(415);
        }
        assertThat(run("POST", "APPLICATION/JSON", null).reached()).as("plain upper case is still application/json").isTrue();
    }

    @Test
    void putPatchAndDeleteAreHeldToTheSameRule() throws Exception {
        for (String method : new String[] {"PUT", "PATCH", "DELETE", "post", "CUSTOM"}) {
            assertThat(run(method, "application/x-www-form-urlencoded", null).status()).as(method).isEqualTo(415);
            assertThat(run(method, "application/json", null).reached()).as(method + " json").isTrue();
        }
    }

    @Test
    void aRequestTheBrowserMarksCrossSiteOrSameSiteIsRefusedEvenWithJson() throws Exception {
        // a browser sends these tokens in lower case; anything else, including odd casing, is not trusted
        for (String site : new String[] {"cross-site", "same-site", "Cross-Site", " cross-site ", "something-new", "Same-Origin", "NONE", ""}) {
            Outcome o = run("POST", "application/json", site);
            assertThat(o.status()).as("Sec-Fetch-Site: " + site).isEqualTo(HttpServletResponse.SC_FORBIDDEN);
            assertThat(o.reached()).as(site).isFalse();
        }
    }

    @Test
    void sameOriginAndUserInitiatedRequestsAndNonBrowsersAreAllowed() throws Exception {
        assertThat(run("POST", "application/json", "same-origin").reached()).isTrue();
        assertThat(run("POST", "application/json", "none").reached()).isTrue();
        assertThat(run("POST", "application/json", null).reached()).as("curl and scripts send no Sec-Fetch-Site").isTrue();
    }

    @Test
    void theRefusalIsAJsonErrorWithNothingSecretInIt() throws Exception {
        Outcome o = run("POST", "text/plain", null);

        assertThat(o.body()).isEqualTo("{\"error\":\"state-changing requests must send Content-Type: application/json\"}");
        assertThat(run("POST", "application/json", "cross-site").body())
                .isEqualTo("{\"error\":\"requests from another site may not change state\"}");
    }
}
