package com.example.dlqagent.support;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.MessageType;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.Prompt;

/** Deterministic stand-in for an LLM: replies with whatever the responder returns, and records prompts. */
public class ScriptedChatModel implements ChatModel {

    private volatile Function<Prompt, String> responder;
    private final List<Prompt> prompts = new CopyOnWriteArrayList<>();

    public ScriptedChatModel(Function<Prompt, String> responder) {
        this.responder = responder;
    }

    public static ScriptedChatModel replying(String text) {
        return new ScriptedChatModel(p -> text);
    }

    public static String decision(String strategy, double confidence, String rationale) {
        return "{\"strategy\":\"%s\",\"confidence\":%s,\"rationale\":\"%s\"}".formatted(strategy, confidence, rationale);
    }

    public void respondWith(Function<Prompt, String> responder) {
        this.responder = responder;
    }

    public List<Prompt> prompts() {
        return prompts;
    }

    /** Text of the system message(s), which identifies which agent (planner or repairer) is calling. */
    public static String systemText(Prompt prompt) {
        return prompt.getInstructions().stream()
                .filter(m -> m.getMessageType() == MessageType.SYSTEM)
                .map(m -> m.getText())
                .reduce("", String::concat);
    }

    public static boolean isPlanner(Prompt prompt) {
        return systemText(prompt).contains("remediation planner");
    }

    public static boolean isRepairer(Prompt prompt) {
        return systemText(prompt).contains("repair agent");
    }

    public Prompt lastPrompt() {
        return prompts.get(prompts.size() - 1);
    }

    @Override
    public ChatResponse call(Prompt prompt) {
        prompts.add(prompt);
        return new ChatResponse(List.of(new Generation(new AssistantMessage(responder.apply(prompt)))));
    }
}
