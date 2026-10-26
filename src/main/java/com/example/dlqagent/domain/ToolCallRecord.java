package com.example.dlqagent.domain;

/** Summary of one tool call the repair model made. Deliberately has no arguments or results: they can hold payload data. */
public record ToolCallRecord(String tool, boolean succeeded, long durationMs) {}
