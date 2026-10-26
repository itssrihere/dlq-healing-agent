package com.example.dlqagent.domain;

/** Port: publishes a repaired event back to its original topic with audit metadata. */
public interface EventReplayer {

    /** @throws RuntimeException if the broker did not acknowledge the record (caller should retry) */
    void replay(ReplayRequest request);
}
