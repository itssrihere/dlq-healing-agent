package com.example.dlqagent.agent;

import java.util.List;

/**
 * The repair agent's structured answer. It is a proposal, not an action: Java applies the edits
 * ({@link PatchApplier}) and the {@link EvaluatorGate} decides whether the result may be replayed.
 *
 * @param repairable false if the event cannot be fixed without guessing
 * @param confidence 0 to 1; only ever used to demote a repair to human approval, never to promote it
 * @param summary    what was wrong and what was changed, including any assumption made
 * @param edits      field edits (whitelisted fields only)
 * @param dropFields names of unknown fields to remove from the original (e.g. a renamed field's old name)
 */
record PatchProposal(boolean repairable, double confidence, String summary, List<FieldEdit> edits, List<String> dropFields) {

    PatchProposal {
        summary = summary == null ? "" : summary;
        edits = edits == null ? List.of() : List.copyOf(edits);
        dropFields = dropFields == null ? List.of() : List.copyOf(dropFields);
    }
}
