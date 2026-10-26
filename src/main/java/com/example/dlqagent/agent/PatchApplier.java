package com.example.dlqagent.agent;

import com.example.dlqagent.domain.TopicAdapter;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import org.springframework.stereotype.Component;

/**
 * Applies a {@link PatchProposal} to a copy of the original payload. Deterministic, bounded and
 * whitelist-only: what may be edited or dropped is decided by the topic adapter, never by the model.
 */
@Component
class PatchApplier {

    static final int MAX_EDITS = 8;
    static final int MAX_VALUE_CHARS = 64;

    ObjectNode apply(TopicAdapter adapter, ObjectNode original, PatchProposal proposal) {
        if (proposal.edits().size() > MAX_EDITS || proposal.dropFields().size() > MAX_EDITS) {
            throw new PatchRejectedException("too many edits (max " + MAX_EDITS + ")");
        }
        ObjectNode patched = original.deepCopy();

        for (String field : proposal.dropFields()) {
            if (adapter.immutableFields().contains(field)) {
                throw new PatchRejectedException("cannot drop identity field '" + field + "'");
            }
            if (!patched.has(field)) {
                throw new PatchRejectedException("cannot drop '" + field + "': not in the original");
            }
            patched.remove(field);
        }

        Set<String> seen = new HashSet<>();
        for (FieldEdit edit : proposal.edits()) {
            if (edit == null || edit.field() == null || edit.newValue() == null) {
                throw new PatchRejectedException("malformed edit");
            }
            TopicAdapter.FieldType type = adapter.editableFields().get(edit.field());
            if (type == null) {
                throw new PatchRejectedException("field '" + edit.field() + "' is not editable");
            }
            if (!seen.add(edit.field())) {
                throw new PatchRejectedException("duplicate edit for '" + edit.field() + "'");
            }
            String value = edit.newValue().trim();
            if (value.isEmpty() || value.length() > MAX_VALUE_CHARS) {
                throw new PatchRejectedException("edit value for '" + edit.field() + "' is empty or too long");
            }
            switch (type) {
                case DECIMAL -> {
                    try {
                        patched.set(edit.field(), DecimalNode.valueOf(new BigDecimal(value)));
                    } catch (NumberFormatException e) {
                        throw new PatchRejectedException(
                                "'" + edit.field() + "' value '" + value + "' is not a plain number");
                    }
                }
                case TEXT -> patched.set(edit.field(), TextNode.valueOf(value));
            }
        }
        return patched;
    }
}
