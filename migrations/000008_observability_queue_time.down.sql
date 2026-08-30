BEGIN;

ALTER TABLE tasks
    DROP CONSTRAINT IF EXISTS tasks_queued_at_ordered,
    DROP COLUMN IF EXISTS queued_at;

DELETE FROM schema_migrations
WHERE version = '000008_observability_queue_time';

COMMIT;
