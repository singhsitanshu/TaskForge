BEGIN;

ALTER TABLE tasks
    DROP CONSTRAINT tasks_worker_claim_shape;

UPDATE tasks
SET
    leased_by_worker_id = claimed_by_worker_id,
    lease_token = COALESCE(lease_token, gen_random_uuid()),
    lease_expires_at = COALESCE(
        lease_expires_at,
        GREATEST(clock_timestamp(), updated_at) + interval '5 minutes'
    )
WHERE status = 'RUNNING';

UPDATE tasks
SET
    status = CASE
        WHEN status = 'LEASED' THEN 'QUEUED'::task_status
        ELSE status
    END,
    leased_by_worker_id = NULL,
    lease_token = NULL,
    lease_expires_at = NULL
WHERE status <> 'RUNNING';

UPDATE task_attempts
SET
    lease_token = COALESCE(lease_token, gen_random_uuid()),
    lease_expires_at = COALESCE(
        lease_expires_at,
        GREATEST(leased_at, COALESCE(finished_at, clock_timestamp()))
            + interval '1 second'
    )
WHERE lease_token IS NULL
   OR lease_expires_at IS NULL;

DROP INDEX IF EXISTS tasks_claimed_worker_idx;

ALTER TABLE tasks
    DROP COLUMN claimed_by_worker_id;

ALTER TABLE tasks
    ADD CONSTRAINT tasks_lease_shape CHECK (
        (
            status IN ('LEASED', 'RUNNING')
            AND leased_by_worker_id IS NOT NULL
            AND lease_token IS NOT NULL
            AND lease_expires_at IS NOT NULL
        )
        OR
        (
            status NOT IN ('LEASED', 'RUNNING')
            AND leased_by_worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
        )
    );

ALTER TABLE task_attempts
    ALTER COLUMN lease_token SET NOT NULL,
    ALTER COLUMN lease_expires_at SET NOT NULL;

DELETE FROM schema_migrations
WHERE version = '000002_first_worker_claim';

COMMIT;
