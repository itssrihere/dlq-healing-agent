package com.example.dlqagent.contract;

/**
 * One property of the contract, as far as the change rules need to know it.
 *
 * @param kind   the JSON Schema type; anything not a plain string or number is {@code OTHER} and never editable
 * @param format the schema's {@code format} (for example {@code date-time}), or null
 */
record FieldSpec(String name, Kind kind, String format) {

    enum Kind { STRING, NUMBER, INTEGER, OTHER }

    boolean isNumeric() {
        return kind == Kind.NUMBER || kind == Kind.INTEGER;
    }

    boolean isDateTime() {
        return kind == Kind.STRING && ("date-time".equals(format) || "date".equals(format));
    }
}
