package com.example.dlqagent.infrastructure;

import static org.springframework.security.config.Customizer.withDefaults;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.AuthorizationFilter;

/**
 * Approving a Tier 2 proposal publishes to a production topic, so the review API is authenticated and
 * everything not explicitly allowed is denied. One configured reviewer account; the password has no default.
 */
@Configuration
class SecurityConfig {

    static final String ROLE = "APPROVER";

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                // CSRF tokens are off because this is a stateless Basic-auth API for scripts, not a browser app. Be
                // aware that a browser can cache Basic credentials and resend them cross-site, so "no cookies" is not
                // by itself a defence. The defence is StateChangingRequestGuard, added below, which applies to EVERY
                // request that is not a read: it must be application/json (a cross-site HTML form cannot send that, and a
                // cross-site fetch with it needs a CORS preflight this service never approves; there is no CORS
                // configuration) and must not be marked cross-site by the browser (Sec-Fetch-Site). The controllers' own
                // consumes = application/json is a second layer.
                .csrf(csrf -> csrf.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(a -> a
                        .requestMatchers("/actuator/health", "/actuator/health/**").permitAll()
                        .requestMatchers("/api/approvals/**", "/api/audit/**").hasRole(ROLE)
                        .anyRequest().denyAll())
                .httpBasic(withDefaults())
                // After authorisation on purpose: strangers still get a plain 401, and a forged cross-site request,
                // which arrives with the browser's cached credentials, is stopped here.
                .addFilterAfter(new StateChangingRequestGuard(), AuthorizationFilter.class);
        return http.build();
    }

    @Bean
    PasswordEncoder passwordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }

    @Bean
    UserDetailsService approvers(DlqAgentProperties props, PasswordEncoder encoder) {
        return new InMemoryUserDetailsManager(User.withUsername(props.approval().username())
                .password(encoder.encode(props.approval().password()))
                .roles(ROLE)
                .build());
    }
}
