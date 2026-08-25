BEGIN;

DROP INDEX task_attempts_worker_active_idx;

ALTER TABLE task_attempts
    DROP CONSTRAINT task_attempts_lease_token_unique,
    DROP CONSTRAINT task_attempts_timestamps_ordered,
    DROP COLUMN lease_token,
    DROP COLUMN lease_expires_at;

ALTER TABLE task_attempts
    ADD CONSTRAINT task_attempts_timestamps_ordered CHECK (
        created_at <= updated_at
        AND (started_at IS NULL OR started_at >= leased_at)
        AND (finished_at IS NULL OR finished_at >= leased_at)
    );

DROP INDEX tasks_expired_lease_idx;

ALTER TABLE tasks
    DROP CONSTRAINT tasks_worker_claim_shape,
    DROP CONSTRAINT tasks_timestamps_ordered,
    DROP CONSTRAINT tasks_lease_token_unique,
    DROP COLUMN leased_by_worker_id,
    DROP COLUMN lease_token;

-- Preserve stranded pre-TF-007 RUNNING work as expired, unrecovered ownership.
UPDATE tasks
SET lease_expires_at = updated_at + interval '1 microsecond'
WHERE status = 'RUNNING'
  AND lease_expires_at IS NULL;

ALTER TABLE tasks
    ADD CONSTRAINT tasks_timestamps_ordered CHECK (
        updated_at >= created_at
        AND (completed_at IS NULL OR completed_at >= created_at)
    );

ALTER TABLE tasks
    ADD CONSTRAINT tasks_worker_claim_shape CHECK (
        (
            status = 'RUNNING'
            AND claimed_by_worker_id IS NOT NULL
            AND lease_expires_at IS NOT NULL
        )
        OR
        (
            status <> 'RUNNING'
            AND claimed_by_worker_id IS NULL
            AND lease_expires_at IS NULL
        )
    );

CREATE INDEX tasks_running_lease_idx
    ON tasks (lease_expires_at)
    WHERE status = 'RUNNING';

INSERT INTO schema_migrations (version)
VALUES ('000004_task_leases');

COMMIT;
