-- Several DLQs are healed by one service. Every held event, retry and decision records which stream it belongs to,
-- so each can be listed, counted and (later) authorised per stream.
--
-- Rows written before streams existed all belong to the only stream there was, which was named "orders". The default
-- exists only to backfill them; it is dropped straight away so a new insert must name its stream.
ALTER TABLE quarantine_event ADD COLUMN stream VARCHAR(64) NOT NULL DEFAULT 'orders';
ALTER TABLE retry_schedule   ADD COLUMN stream VARCHAR(64) NOT NULL DEFAULT 'orders';
ALTER TABLE dlq_audit_log    ADD COLUMN stream VARCHAR(64) NOT NULL DEFAULT 'orders';
ALTER TABLE quarantine_event ALTER COLUMN stream DROP DEFAULT;
ALTER TABLE retry_schedule   ALTER COLUMN stream DROP DEFAULT;
ALTER TABLE dlq_audit_log    ALTER COLUMN stream DROP DEFAULT;

CREATE INDEX idx_quarantine_stream_status ON quarantine_event (stream, status, created_at DESC);
CREATE INDEX idx_retry_stream_due         ON retry_schedule (stream, status, next_attempt_at);
CREATE INDEX idx_audit_stream             ON dlq_audit_log (stream, occurred_at);
