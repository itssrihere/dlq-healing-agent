-- Events the agent could not (or must not) repair. Held for human review.
CREATE TABLE quarantine_event (
    id             UUID PRIMARY KEY,
    correlation_id VARCHAR(64)  NOT NULL,
    original_topic VARCHAR(255) NOT NULL,
    kafka_key      VARCHAR(255),
    payload        TEXT         NOT NULL,
    headers        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    stack_trace    TEXT,
    strategy       VARCHAR(32)  NOT NULL,
    reason         TEXT         NOT NULL,
    status         VARCHAR(32)  NOT NULL DEFAULT 'PENDING_REVIEW',
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    reviewed_at    TIMESTAMPTZ,
    reviewed_by    VARCHAR(255),
    review_notes   TEXT,
    CONSTRAINT chk_quarantine_status CHECK (status IN ('PENDING_REVIEW', 'RESOLVED', 'DISCARDED'))
);

CREATE INDEX idx_quarantine_status_created ON quarantine_event (status, created_at);
CREATE INDEX idx_quarantine_correlation ON quarantine_event (correlation_id);
