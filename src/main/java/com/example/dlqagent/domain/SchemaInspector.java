package com.example.dlqagent.domain;

import java.util.List;

/** Port: read-only view of a database table's structure. */
public interface SchemaInspector {

    record Column(String name, String type, boolean nullable, Integer maxLength,
                  Integer numericPrecision, Integer numericScale) {}

    record TableSchema(String table, List<Column> columns, List<String> constraints) {

        public TableSchema {
            columns = List.copyOf(columns);
            constraints = List.copyOf(constraints);
        }
    }

    /** @throws IllegalArgumentException if the table is not on the allow-list */
    TableSchema describe(String table);
}
