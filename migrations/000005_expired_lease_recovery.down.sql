BEGIN;

-- PostgreSQL enum labels are additive. Convert durable recovery history before
-- restoring the pre-TF-008 constraints; the complete rollback in 000001 drops
-- and recreates the enum itself.
UPDATE task_attempts
SET
    status = 'FAILED',
    error = COALESCE(error, 'lease_expired')
WHERE status = 'ABANDONED';

DROP INDEX IF EXISTS task_attempts_finished_idx;

ALTER TABLE task_attempts
    DROP CONSTRAINT task_attempts_completion_shape,
    DROP CONSTRAINT task_attempts_status_allowed,
    ADD CONSTRAINT task_attempts_status_allowed CHECK (
        status IN ('LEASED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')
    ),
    ADD CONSTRAINT task_attempts_completion_shape CHECK (
        (
            status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
            AND finished_at IS NOT NULL
        )
        OR
        (
            status IN ('LEASED', 'RUNNING')
            AND finished_at IS NULL
        )
    );

CREATE INDEX task_attempts_finished_idx
    ON task_attempts (status, finished_at DESC)
    WHERE status IN ('SUCCEEDED', 'FAILED', 'CANCELLED');

ALTER TABLE tasks
    DROP CONSTRAINT tasks_status_allowed;

DELETE FROM schema_migrations
WHERE version = '000005_expired_lease_recovery';

COMMIT;
