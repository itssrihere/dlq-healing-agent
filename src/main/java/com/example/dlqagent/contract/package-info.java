/**
 * Schema-driven Strategy A: a topic's contract is one JSON Schema document plus a small policy. The shape check, the
 * editable fields, the model's prompt and the rules that a repair changes only the representation of a value, never
 * its meaning, are all derived from it. Nothing here knows about orders.
 */
package com.example.dlqagent.contract;
