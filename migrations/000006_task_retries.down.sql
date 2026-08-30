BEGIN;

DROP INDEX IF EXISTS tasks_retry_due_idx;

DELETE FROM schema_migrations
WHERE version = '000006_task_retries';

COMMIT;
