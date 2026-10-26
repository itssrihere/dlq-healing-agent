package com.example.dlqagent.domain;

/** One top-level field that differs between two payloads. {@code before} or {@code after} is null if absent. */
public record FieldChange(String field, String before, String after) {}
