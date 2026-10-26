package com.example.dlqagent.support;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.ai.chat.model.ChatModel;

/** Replaces the real provider's ChatModel in tests so no network call or API key is ever needed. */
@TestConfiguration
@Profile("!live-llm")
public class StubLlmConfig {

    @Bean
    @Primary
    ScriptedChatModel scriptedChatModel() {
        return ScriptedChatModel.replying(ScriptedChatModel.decision("POISON_PILL", 0.95, "stubbed poison pill"));
    }
}
