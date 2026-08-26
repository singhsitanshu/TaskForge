BEGIN;

DROP INDEX IF EXISTS tasks_idempotency_key_idx;

ALTER TABLE tasks
    DROP CONSTRAINT IF EXISTS tasks_request_fingerprint_sha256,
    DROP CONSTRAINT IF EXISTS tasks_idempotency_fingerprint_pair,
    DROP COLUMN IF EXISTS request_fingerprint;

CREATE UNIQUE INDEX tasks_queue_idempotency_key_idx
    ON tasks (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

DELETE FROM schema_migrations
WHERE version = '000007_submission_idempotency';

COMMIT;
