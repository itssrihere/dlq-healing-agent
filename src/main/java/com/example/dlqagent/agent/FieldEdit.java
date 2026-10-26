package com.example.dlqagent.agent;

/** Set one field to a new value. The field must be on the topic adapter's editable list; the value is text. */
record FieldEdit(String field, String newValue) {}
