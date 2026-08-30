BEGIN;

ALTER TABLE task_attempts
    DROP CONSTRAINT task_attempts_recovery_evidence_shape,
    DROP CONSTRAINT task_attempts_queue_timing_ordered,
    DROP COLUMN recovery_action,
    DROP COLUMN recovered_at,
    DROP COLUMN recovered_lease_expires_at,
    DROP COLUMN retry_scheduled_at,
    DROP COLUMN scheduled_at_snapshot,
    DROP COLUMN queue_entered_at;

DELETE FROM schema_migrations
WHERE version = '000010_attempt_timing_evidence';

COMMIT;
