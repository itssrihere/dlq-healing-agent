package com.example.dlqagent.domain;

/**
 * Port: where a topic's contract (a JSON Schema document) comes from. Today a classpath or file resource; a schema
 * registry would be another implementation, so the engine never needs to know where a contract lives.
 */
public interface ContractSource {

    /** @return the schema document text @throws IllegalStateException if it cannot be read */
    String load(String location);
}
