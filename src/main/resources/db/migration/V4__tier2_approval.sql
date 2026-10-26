-- Tier 2: an agent-proposed repair waits here for a human (status PENDING_HUMAN_APPROVAL).
-- Poison pills and rejected patches keep using this table with status PENDING_REVIEW.
ALTER TABLE quarantine_event DROP CONSTRAINT chk_quarantine_status;
ALTER TABLE quarantine_event ADD CONSTRAINT chk_quarantine_status CHECK (status IN
    ('PENDING_REVIEW', 'PENDING_HUMAN_APPROVAL', 'APPROVED_REPLAYED', 'REJECTED', 'RESOLVED', 'DISCARDED'));

ALTER TABLE quarantine_event
    ADD COLUMN proposed_payload TEXT,
    ADD COLUMN repair_summary   TEXT,
    ADD COLUMN risk_flags       JSONB NOT NULL DEFAULT '[]'::jsonb;

-- A pending approval is meaningless without the payload to approve.
ALTER TABLE quarantine_event ADD CONSTRAINT chk_pending_approval_has_proposal
    CHECK (status <> 'PENDING_HUMAN_APPROVAL' OR proposed_payload IS NOT NULL);
