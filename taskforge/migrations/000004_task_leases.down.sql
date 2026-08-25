BEGIN;

DROP INDEX IF EXISTS tasks_running_lease_idx;

ALTER TABLE tasks
    DROP CONSTRAINT tasks_worker_claim_shape,
    DROP CONSTRAINT tasks_timestamps_ordered;

UPDATE tasks
SET
    claimed_by_worker_id = NULL,
    lease_expires_at = NULL,
    status = CASE
        WHEN status = 'RUNNING' THEN 'QUEUED'::task_status
        ELSE status
    END;

ALTER TABLE tasks
    ADD COLUMN leased_by_worker_id uuid REFERENCES workers (id) ON DELETE RESTRICT,
    ADD COLUMN lease_token uuid,
    ADD CONSTRAINT tasks_lease_token_unique UNIQUE (lease_token),
    ADD CONSTRAINT tasks_worker_claim_shape CHECK (
        leased_by_worker_id IS NULL
        AND lease_token IS NULL
        AND lease_expires_at IS NULL
        AND (
            (status = 'RUNNING' AND claimed_by_worker_id IS NOT NULL)
            OR
            (status <> 'RUNNING' AND claimed_by_worker_id IS NULL)
        )
    ),
    ADD CONSTRAINT tasks_timestamps_ordered CHECK (
        updated_at >= created_at
        AND (lease_expires_at IS NULL OR lease_expires_at > updated_at)
        AND (completed_at IS NULL OR completed_at >= created_at)
    );

CREATE INDEX tasks_expired_lease_idx
    ON tasks (lease_expires_at)
    WHERE status IN ('LEASED', 'RUNNING');

ALTER TABLE task_attempts
    DROP CONSTRAINT task_attempts_timestamps_ordered,
    ADD COLUMN lease_token uuid,
    ADD COLUMN lease_expires_at timestamptz,
    ADD CONSTRAINT task_attempts_lease_token_unique UNIQUE (lease_token),
    ADD CONSTRAINT task_attempts_timestamps_ordered CHECK (
        lease_expires_at > leased_at
        AND created_at <= updated_at
        AND (started_at IS NULL OR started_at >= leased_at)
        AND (finished_at IS NULL OR finished_at >= leased_at)
    );

CREATE INDEX task_attempts_worker_active_idx
    ON task_attempts (worker_id, lease_expires_at)
    WHERE status IN ('LEASED', 'RUNNING');

DELETE FROM schema_migrations
WHERE version = '000004_task_leases';

COMMIT;
