BEGIN;

CREATE INDEX tasks_retry_due_idx
    ON tasks (scheduled_at, id)
    WHERE status = 'RETRYING';

INSERT INTO schema_migrations (version)
VALUES ('000006_task_retries');

COMMIT;
