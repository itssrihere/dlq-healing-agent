-- Strategy B (transient retry). retry_schedule was created in V2 but unused until now, so it holds no rows and
-- the NOT NULL columns below can be added without a default.

-- Identity of a retry is the content of the event (topic + key + payload), not a header: the payload is replayed
-- unchanged, so it is the one thing that is still the same when the event fails again, even if the main service's
-- dead-letter publisher drops or rewrites our headers.
ALTER TABLE retry_schedule ADD COLUMN fingerprint   CHAR(64) NOT NULL;
-- The DLQ record that last scheduled this retry. Redelivery of that record must not count as a new failure.
ALTER TABLE retry_schedule ADD COLUMN last_event_id UUID     NOT NULL;
CREATE UNIQUE INDEX uq_retry_fingerprint ON retry_schedule (fingerprint);

-- attempt = number of replays already sent. A due row is claimed and replayed inside one transaction, so there is
-- no IN_FLIGHT state: a crash rolls the claim back and the row is simply due again. REPLAYED means "replayed, and
-- nothing has come back to the DLQ for it (yet)"; if it does come back, the row is rescheduled or exhausted.
ALTER TABLE retry_schedule DROP CONSTRAINT chk_retry_status;
ALTER TABLE retry_schedule ADD CONSTRAINT chk_retry_status CHECK (status IN ('SCHEDULED', 'REPLAYED', 'EXHAUSTED'));

-- Audit: a scheduled retry and a retry replay are new kinds of outcome, and the replay happens in a new stage.
ALTER TABLE dlq_audit_log DROP CONSTRAINT chk_audit_stage;
ALTER TABLE dlq_audit_log ADD CONSTRAINT chk_audit_stage CHECK (stage IN ('TRIAGE', 'APPROVAL', 'RETRY'));
ALTER TABLE dlq_audit_log DROP CONSTRAINT chk_audit_outcome;
ALTER TABLE dlq_audit_log ADD CONSTRAINT chk_audit_outcome CHECK (outcome IN
    ('REPLAYED', 'HELD_FOR_APPROVAL', 'QUARANTINED', 'APPROVED_REPLAYED', 'REJECTED',
     'RETRY_SCHEDULED', 'RETRY_REPLAYED'));
