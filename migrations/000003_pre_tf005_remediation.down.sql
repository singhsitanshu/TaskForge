BEGIN;

DROP INDEX IF EXISTS tasks_claim_priority_idx;
DROP INDEX IF EXISTS workers_name_idx;

UPDATE workers AS worker
SET name = left(worker.name, 218) || '-' || worker.id::text
WHERE EXISTS (
    SELECT 1
    FROM workers AS duplicate
    WHERE duplicate.name = worker.name
      AND duplicate.id <> worker.id
);

ALTER TABLE workers
    ADD CONSTRAINT workers_name_unique UNIQUE (name),
    DROP CONSTRAINT workers_instance_id_unique,
    DROP CONSTRAINT workers_instance_id_not_blank,
    DROP COLUMN instance_id;

DELETE FROM schema_migrations
WHERE version = '000003_pre_tf005_remediation';

COMMIT;
