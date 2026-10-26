-- One row per execution: every triage outcome and every human decision. Append-only.
-- Holds reasoning summaries, tool-call summaries and field-level diffs; never raw prompts or tool arguments.
CREATE TABLE dlq_audit_log (
    id                 UUID PRIMARY KEY,
    event_id           UUID         NOT NULL,   -- identity of the DLQ record (topic, partition, offset)
    correlation_id     VARCHAR(64)  NOT NULL,
    trace_id           VARCHAR(64),             -- joins this row to the distributed trace
    occurred_at        TIMESTAMPTZ  NOT NULL,
    stage              VARCHAR(16)  NOT NULL,
    original_topic     VARCHAR(255) NOT NULL,
    kafka_key          VARCHAR(255),
    strategy           VARCHAR(32)  NOT NULL,
    planner_confidence DOUBLE PRECISION,
    planner_rationale  TEXT,
    repair_summary     TEXT,
    repair_confidence  DOUBLE PRECISION,
    tool_calls         JSONB        NOT NULL DEFAULT '[]'::jsonb,
    changes            JSONB        NOT NULL DEFAULT '[]'::jsonb,
    risk_flags         JSONB        NOT NULL DEFAULT '[]'::jsonb,
    tier               VARCHAR(16)  NOT NULL,
    outcome            VARCHAR(32)  NOT NULL,
    outcome_reason     TEXT,
    actor              VARCHAR(255) NOT NULL,   -- 'agent' or the reviewer's username
    duration_ms        BIGINT       NOT NULL,
    CONSTRAINT chk_audit_stage   CHECK (stage IN ('TRIAGE', 'APPROVAL')),
    CONSTRAINT chk_audit_tier    CHECK (tier IN ('NONE', 'TIER_1', 'TIER_2')),
    CONSTRAINT chk_audit_outcome CHECK (outcome IN
        ('REPLAYED', 'HELD_FOR_APPROVAL', 'QUARANTINED', 'APPROVED_REPLAYED', 'REJECTED'))
);

CREATE INDEX idx_audit_correlation ON dlq_audit_log (correlation_id, occurred_at);
CREATE INDEX idx_audit_event ON dlq_audit_log (event_id);

-- An audit log that can be edited is not an audit log. (A superuser can still drop the trigger.)
CREATE FUNCTION forbid_audit_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'dlq_audit_log is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER dlq_audit_log_append_only
    BEFORE UPDATE OR DELETE ON dlq_audit_log
    FOR EACH ROW EXECUTE FUNCTION forbid_audit_mutation();
