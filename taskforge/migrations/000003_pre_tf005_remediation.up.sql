BEGIN;

ALTER TABLE workers
    ADD COLUMN instance_id varchar(255);

UPDATE workers
SET instance_id = name;

ALTER TABLE workers
    ALTER COLUMN instance_id SET NOT NULL,
    ADD CONSTRAINT workers_instance_id_not_blank CHECK (btrim(instance_id) <> ''),
    ADD CONSTRAINT workers_instance_id_unique UNIQUE (instance_id),
    DROP CONSTRAINT workers_name_unique;

CREATE INDEX workers_name_idx
    ON workers (name);

CREATE INDEX tasks_claim_priority_idx
    ON tasks (priority DESC, created_at ASC, id ASC)
    WHERE status = 'QUEUED';

INSERT INTO schema_migrations (version)
VALUES ('000003_pre_tf005_remediation');

COMMIT;
