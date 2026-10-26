-- Durable store for Strategy B (transient failure) exponential-backoff retries.
CREATE TABLE retry_schedule (
    id              UUID PRIMARY KEY,
    correlation_id  VARCHAR(64)  NOT NULL,
    target_topic    VARCHAR(255) NOT NULL,
    kafka_key       VARCHAR(255),
    payload         TEXT         NOT NULL,
    headers         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    attempt         INT          NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ  NOT NULL,
    status          VARCHAR(32)  NOT NULL DEFAULT 'SCHEDULED',
    last_error      TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_retry_status CHECK (status IN ('SCHEDULED', 'IN_FLIGHT', 'DONE', 'EXHAUSTED'))
);

-- The poller scans for due work.
CREATE INDEX idx_retry_due ON retry_schedule (status, next_attempt_at);
CREATE INDEX idx_retry_correlation ON retry_schedule (correlation_id);
