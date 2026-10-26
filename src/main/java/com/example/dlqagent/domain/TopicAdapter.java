package com.example.dlqagent.domain;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The boundary between the generic self-healing engine and one topic's domain. The engine knows nothing
 * about orders or any other payload; everything topic-specific is supplied here:
 * the contract the model is shown, the tools it may call, which fields a patch may touch, and the
 * deterministic rules a repaired payload must satisfy before it can be replayed.
 */
public interface TopicAdapter {

    /** How an edit's text value is converted before it is written into the payload. */
    enum FieldType { TEXT, DECIMAL }

    /** The topic this adapter serves; also the only topic repaired events may be replayed to. */
    String topic();

    /** Prompt text describing the target contract: fields, formats, constraints, tool names. */
    String contract();

    /** JSON field names a patch may set, with their types. Everything else is off limits to the model. */
    Map<String, FieldType> editableFields();

    /** Identity fields: never editable, never droppable, and must be unchanged in a repaired payload. */
    Set<String> immutableFields();

    /** Standalone contract check of one payload. Empty means valid. Also exposed to the model as a tool. */
    List<String> validate(String json);

    /**
     * Authoritative, deterministic check of a candidate repair against the original failed payload.
     * Violations are hard failures; risk flags mark valid-but-assumption-laden repairs for human approval.
     * The model's own use of tools is never trusted instead of this.
     */
    Evaluation evaluate(String originalJson, String candidateJson, String kafkaKey);

    /** Tool objects (classes with {@code @Tool} methods) the repair model may call for this topic. */
    List<Object> tools();
}
