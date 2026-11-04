package com.example.dlqagent.retry;

import com.example.dlqagent.infrastructure.DlqAgentProperties;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

/** Wires the retry poller to its interval from configuration, and provides the clock. */
@Configuration
@EnableScheduling
class RetryConfiguration implements SchedulingConfigurer {

    private final RetryPoller poller;
    private final DlqAgentProperties props;

    RetryConfiguration(RetryPoller poller, DlqAgentProperties props) {
        this.poller = poller;
        this.props = props;
    }

    // Static, so the beans the poller needs do not depend on this class, which itself needs the poller.
    @Bean
    static Clock clock() {
        return Clock.systemUTC();
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        registrar.addFixedDelayTask(poller::poll, props.retry().pollInterval());
    }
}
