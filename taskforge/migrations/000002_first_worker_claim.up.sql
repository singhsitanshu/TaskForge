BEGIN;

ALTER TABLE tasks
    ADD COLUMN claimed_by_worker_id uuid REFERENCES workers (id) ON DELETE RESTRICT;

UPDATE tasks
SET claimed_by_worker_id = leased_by_worker_id
WHERE status = 'RUNNING';

ALTER TABLE tasks
    DROP CONSTRAINT tasks_lease_shape;

UPDATE tasks
SET
    status = CASE
        WHEN status = 'LEASED' THEN 'QUEUED'::task_status
        ELSE status
    END,
    leased_by_worker_id = NULL,
    lease_token = NULL,
    lease_expires_at = NULL;

ALTER TABLE tasks
    ADD CONSTRAINT tasks_worker_claim_shape CHECK (
        (
            leased_by_worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
        )
        AND
        (
            (
                status = 'RUNNING'
                AND claimed_by_worker_id IS NOT NULL
            )
            OR
            (
                status <> 'RUNNING'
                AND claimed_by_worker_id IS NULL
            )
        )
    );

ALTER TABLE task_attempts
    ALTER COLUMN lease_token DROP NOT NULL,
    ALTER COLUMN lease_expires_at DROP NOT NULL;

UPDATE task_attempts
SET
    lease_token = NULL,
    lease_expires_at = NULL;

CREATE INDEX tasks_claimed_worker_idx
    ON tasks (claimed_by_worker_id)
    WHERE status = 'RUNNING';

COMMIT;
