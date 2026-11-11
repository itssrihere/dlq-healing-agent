package com.example.dlqagent.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.dlqagent.support.TestProps;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * The real security configuration in front of an endpoint that forgot to declare {@code consumes}. That forgetful endpoint is
 * the point: the cross-site protection must hold for any endpoint added later, not only for the two that happen to declare it.
 * A slice test (no Kafka, no database), so it adds no second application context to the integration suite.
 */
@WebMvcTest
@ContextConfiguration(classes = {SecurityConfig.class, SecurityChainTest.Probe.class, SecurityChainTest.Props.class})
class SecurityChainTest {

    private static final RequestPostProcessor REVIEWER = httpBasic("reviewer", "unit-test-password");

    @Configuration
    static class Props {
        @Bean
        DlqAgentProperties properties() {
            return TestProps.defaults();
        }
    }

    /** A state-changing endpoint under the protected prefix with NO {@code consumes}: what a future change could add. */
    @RestController
    @RequestMapping("/api/approvals/probe")
    static class Probe {
        static final AtomicInteger CHANGES = new AtomicInteger();

        @RequestMapping(method = {RequestMethod.POST, RequestMethod.PUT, RequestMethod.PATCH, RequestMethod.DELETE})
        String change() {
            CHANGES.incrementAndGet();
            return "changed";
        }

        @GetMapping
        String read() {
            return "read";
        }
    }

    @Autowired MockMvc mvc;

    @BeforeEach
    void reset() {
        Probe.CHANGES.set(0);
    }

    @Test
    void aCrossSiteFormCannotReachAnEndpointThatForgotToDeclareJson() throws Exception {
        // without the guard each of these would return 200 and change state: the endpoint accepts anything
        mvc.perform(post("/api/approvals/probe").with(REVIEWER).contentType(MediaType.APPLICATION_FORM_URLENCODED).content("x=1"))
                .andExpect(status().isUnsupportedMediaType());
        mvc.perform(post("/api/approvals/probe").with(REVIEWER).contentType(MediaType.TEXT_PLAIN).content("{}"))
                .andExpect(status().isUnsupportedMediaType());
        mvc.perform(multipart("/api/approvals/probe").with(REVIEWER)).andExpect(status().isUnsupportedMediaType());
        mvc.perform(post("/api/approvals/probe").with(REVIEWER)).andExpect(status().isUnsupportedMediaType());
        mvc.perform(put("/api/approvals/probe").with(REVIEWER).contentType(MediaType.TEXT_PLAIN).content("x")).andExpect(status().isUnsupportedMediaType());
        mvc.perform(patch("/api/approvals/probe").with(REVIEWER).contentType(MediaType.APPLICATION_FORM_URLENCODED).content("x=1")).andExpect(status().isUnsupportedMediaType());
        mvc.perform(delete("/api/approvals/probe").with(REVIEWER)).andExpect(status().isUnsupportedMediaType());

        assertThat(Probe.CHANGES.get()).as("nothing was changed").isZero();
    }

    @Test
    void aJsonRequestFromAScriptGoesThrough() throws Exception {
        mvc.perform(post("/api/approvals/probe").with(REVIEWER).contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isOk()).andExpect(content().string("changed"));
        mvc.perform(delete("/api/approvals/probe").with(REVIEWER).contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isOk());

        assertThat(Probe.CHANGES.get()).isEqualTo(2);
    }

    @Test
    void aBrowserRequestMarkedCrossSiteIsRefusedEvenWhenItIsJsonAndAuthenticated() throws Exception {
        // the browser resends cached Basic credentials, so the request IS authenticated; the guard is what stops it
        for (String site : new String[] {"cross-site", "same-site"}) {
            mvc.perform(post("/api/approvals/probe").with(REVIEWER).header("Sec-Fetch-Site", site)
                            .contentType(MediaType.APPLICATION_JSON).content("{}"))
                    .andExpect(status().isForbidden());
        }
        mvc.perform(post("/api/approvals/probe").with(REVIEWER).header("Sec-Fetch-Site", "same-origin")
                        .contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isOk());

        assertThat(Probe.CHANGES.get()).isEqualTo(1);
    }

    @Test
    void readsAreUnaffectedEvenFromAnotherSite() throws Exception {
        mvc.perform(get("/api/approvals/probe").with(REVIEWER).header("Sec-Fetch-Site", "cross-site"))
                .andExpect(status().isOk()).andExpect(content().string("read"));
    }

    @Test
    void strangersStillGetAPlain401AndLearnNothingAboutTheContentTypeRule() throws Exception {
        // the guard runs after authentication on purpose
        mvc.perform(post("/api/approvals/probe").contentType(MediaType.APPLICATION_FORM_URLENCODED).content("x=1"))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/api/approvals/probe")).andExpect(status().isUnauthorized());
        mvc.perform(post("/api/approvals/probe").with(httpBasic("reviewer", "wrong")).contentType(MediaType.TEXT_PLAIN).content("x"))
                .andExpect(status().isUnauthorized());
        mvc.perform(get("/api/approvals/probe")).andExpect(status().isUnauthorized());

        assertThat(Probe.CHANGES.get()).isZero();
    }

    @Test
    void anythingOutsideTheListedPathsIsStillDeniedByDefault() throws Exception {
        mvc.perform(post("/api/elsewhere").with(REVIEWER).contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isForbidden());
        mvc.perform(get("/api/elsewhere").with(REVIEWER)).andExpect(status().isForbidden());
    }
}
