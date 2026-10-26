package com.example.dlqagent.domain;

/** Port: Strategy A. Implementations must never throw for bad model output and must gate their result. */
public interface PayloadRepairer {

    RepairResult repair(FailureContext context);
}
