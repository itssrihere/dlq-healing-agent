-- Kafka keys and the original-topic header are producer-controlled and unbounded. A VARCHAR limit here would turn
-- one oversized value into an insert that fails on every retry, blocking its partition for good. Store them whole
-- (replay needs the exact key); the correlation id is the one value we control, validated at the boundary instead.
ALTER TABLE quarantine_event ALTER COLUMN kafka_key TYPE TEXT, ALTER COLUMN original_topic TYPE TEXT;
ALTER TABLE dlq_audit_log    ALTER COLUMN kafka_key TYPE TEXT, ALTER COLUMN original_topic TYPE TEXT;
ALTER TABLE retry_schedule   ALTER COLUMN kafka_key TYPE TEXT, ALTER COLUMN target_topic   TYPE TEXT;
