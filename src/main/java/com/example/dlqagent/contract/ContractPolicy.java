package com.example.dlqagent.contract;

import java.util.Set;

/**
 * What a schema cannot say, and a model must not decide for itself.
 *
 * @param identityFields fields that are never edited or dropped and must be unchanged in a repair
 * @param keyField       the field that must equal the Kafka key, or null
 * @param noFutureFields timestamp fields that may not be later than now (plus a small clock skew)
 */
public record ContractPolicy(Set<String> identityFields, String keyField, Set<String> noFutureFields) {

    public ContractPolicy {
        identityFields = Set.copyOf(identityFields);
        noFutureFields = Set.copyOf(noFutureFields);
    }
}
