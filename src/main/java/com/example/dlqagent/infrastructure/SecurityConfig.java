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
                // Stateless HTTP Basic API: no cookies and no sessions, so there is no ambient browser credential to forge.
                .csrf(csrf -> csrf.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(a -> a
                        .requestMatchers("/actuator/health", "/actuator/health/**").permitAll()
                        .requestMatchers("/api/approvals/**", "/api/audit/**").hasRole(ROLE)
                        .anyRequest().denyAll())
                .httpBasic(withDefaults());
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
