package com.example.dlqagent.domain;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Port: persistent store for events awaiting a human, and for the review decisions made on them. */
public interface QuarantineRepository {

    /**
     * Persists the record. Idempotent on {@link QuarantineRecord#id()}.
     *
     * @return true if a new row was written, false if it already existed
     */
    boolean save(QuarantineRecord record);

    Optional<QuarantineRecord> findById(UUID id);

    /** Loads and row-locks the record until the surrounding transaction ends. Needs an active transaction. */
    Optional<QuarantineRecord> findForUpdate(UUID id);

    /** Newest first. */
    List<QuarantineRecord> findByStatus(QuarantineStatus status, int limit);

    /** Records a review decision. @return false if the row does not exist */
    boolean markReviewed(UUID id, QuarantineStatus status, String reviewer, String notes);
}
