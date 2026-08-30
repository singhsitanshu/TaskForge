BEGIN;

ALTER TABLE tasks
    ADD COLUMN request_fingerprint char(64),
    ADD CONSTRAINT tasks_idempotency_fingerprint_pair CHECK (
        (idempotency_key IS NULL AND request_fingerprint IS NULL)
        OR
        (idempotency_key IS NOT NULL AND request_fingerprint IS NOT NULL)
    ) NOT VALID,
    ADD CONSTRAINT tasks_request_fingerprint_sha256 CHECK (
        request_fingerprint IS NULL
        OR request_fingerprint ~ '^[0-9a-f]{64}$'
    );

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM tasks
        WHERE idempotency_key IS NOT NULL
        GROUP BY idempotency_key
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION
            'cannot migrate to global idempotency keys: duplicate legacy keys exist';
    END IF;
END
$$;

DROP INDEX tasks_queue_idempotency_key_idx;

CREATE UNIQUE INDEX tasks_idempotency_key_idx
    ON tasks (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM tasks
        WHERE idempotency_key IS NOT NULL
          AND request_fingerprint IS NULL
    ) THEN
        ALTER TABLE tasks
            VALIDATE CONSTRAINT tasks_idempotency_fingerprint_pair;
    END IF;
END
$$;

INSERT INTO schema_migrations (version)
VALUES ('000007_submission_idempotency');

COMMIT;
