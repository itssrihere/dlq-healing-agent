/**
 * Streams: one DLQ, its single source topic, its contract and its own listener, circuit breaker and retry budget.
 * The engine resolves a stream per record, so nothing downstream of that knows how many streams exist.
 */
package com.example.dlqagent.stream;
