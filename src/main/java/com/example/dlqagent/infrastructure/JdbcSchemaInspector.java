package com.example.dlqagent.infrastructure;

import com.example.dlqagent.domain.SchemaInspector;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/** Read-only catalog lookups restricted to an allow-list of tables. */
@Component
class JdbcSchemaInspector implements SchemaInspector {

    private final Set<String> allowedTables;
    private final JdbcClient jdbc;

    /** Defense in depth: the union of every stream's tables. Each stream's tool applies its own, narrower list first. */
    JdbcSchemaInspector(JdbcClient jdbc, DlqAgentProperties props) {
        this.jdbc = jdbc;
        this.allowedTables = props.streams().stream().flatMap(s -> s.contract().tables().stream())
                .map(t -> t.trim().toLowerCase(Locale.ROOT)).collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    @Override
    public TableSchema describe(String table) {
        String name = table == null ? "" : table.trim().toLowerCase(Locale.ROOT);
        if (!allowedTables.contains(name)) {
            throw new IllegalArgumentException("Table not allowed: '" + table + "'. Allowed: " + allowedTables);
        }
        List<Column> columns = jdbc.sql("""
                SELECT column_name, data_type, is_nullable, character_maximum_length,
                       numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :t
                ORDER BY ordinal_position
                """)
                .param("t", name)
                .query((rs, i) -> new Column(
                        rs.getString("column_name"),
                        rs.getString("data_type"),
                        "YES".equals(rs.getString("is_nullable")),
                        (Integer) rs.getObject("character_maximum_length"),
                        (Integer) rs.getObject("numeric_precision"),
                        (Integer) rs.getObject("numeric_scale")))
                .list();
        List<String> constraints = jdbc.sql("""
                SELECT pg_get_constraintdef(oid) FROM pg_constraint
                WHERE conrelid = CAST(:t AS regclass) AND contype IN ('c', 'p')
                ORDER BY conname
                """)
                .param("t", "public." + name)
                .query(String.class)
                .list();
        return new TableSchema(name, columns, constraints);
    }
}
