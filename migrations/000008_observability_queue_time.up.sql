BEGIN;

ALTER TABLE tasks
    ADD COLUMN queued_at timestamptz;

UPDATE tasks
SET queued_at = CASE
    WHEN status = 'QUEUED' THEN clock_timestamp()
    ELSE created_at
END;

ALTER TABLE tasks
    ALTER COLUMN queued_at SET DEFAULT now(),
    ALTER COLUMN queued_at SET NOT NULL,
    ADD CONSTRAINT tasks_queued_at_ordered CHECK (queued_at >= created_at);

INSERT INTO schema_migrations (version)
VALUES ('000008_observability_queue_time');

COMMIT;
