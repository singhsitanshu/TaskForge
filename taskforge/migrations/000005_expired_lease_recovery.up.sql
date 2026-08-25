BEGIN;

ALTER TYPE task_status ADD VALUE IF NOT EXISTS 'ABANDONED';

COMMIT;

BEGIN;

ALTER TABLE tasks
    ADD CONSTRAINT tasks_status_allowed CHECK (
        status IN (
            'QUEUED',
            'LEASED',
            'RUNNING',
            'RETRYING',
            'SUCCEEDED',
            'FAILED',
            'CANCELLED'
        )
    );

DROP INDEX task_attempts_finished_idx;

ALTER TABLE task_attempts
    DROP CONSTRAINT task_attempts_status_allowed,
    DROP CONSTRAINT task_attempts_completion_shape,
    ADD CONSTRAINT task_attempts_status_allowed CHECK (
        status IN (
            'LEASED',
            'RUNNING',
            'SUCCEEDED',
            'FAILED',
            'CANCELLED',
            'ABANDONED'
        )
    ),
    ADD CONSTRAINT task_attempts_completion_shape CHECK (
        (
            status IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'ABANDONED')
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
    WHERE status IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'ABANDONED');

INSERT INTO schema_migrations (version)
VALUES ('000005_expired_lease_recovery');

COMMIT;
