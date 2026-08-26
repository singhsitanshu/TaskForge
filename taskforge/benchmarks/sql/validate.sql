WITH duplicate_attempts AS (
    SELECT count(*) AS value
    FROM (
        SELECT task_id, attempt_number
        FROM task_attempts
        GROUP BY task_id, attempt_number
        HAVING count(*) > 1
    ) AS duplicates
), invalid_terminal_tasks AS (
    SELECT count(*) AS value
    FROM tasks
    WHERE status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
      AND (completed_at IS NULL OR lease_expires_at IS NOT NULL OR claimed_by_worker_id IS NOT NULL)
), invalid_active_tasks AS (
    SELECT count(*) AS value
    FROM tasks
    WHERE status IN ('LEASED', 'RUNNING')
      AND (lease_expires_at IS NULL OR claimed_by_worker_id IS NULL)
), invalid_attempt_counts AS (
    SELECT count(*) AS value
    FROM tasks AS task
    WHERE task.attempt_count <> (
        SELECT count(*) FROM task_attempts AS attempt WHERE attempt.task_id = task.id
    )
), invalid_running_attempts AS (
    SELECT count(*) AS value
    FROM task_attempts AS attempt
    JOIN tasks AS task ON task.id = attempt.task_id
    WHERE attempt.status = 'RUNNING'
      AND (task.status <> 'RUNNING' OR task.claimed_by_worker_id <> attempt.worker_id)
)
SELECT json_build_object(
    'duplicate_attempts', (SELECT value FROM duplicate_attempts),
    'invalid_terminal_tasks', (SELECT value FROM invalid_terminal_tasks),
    'invalid_active_tasks', (SELECT value FROM invalid_active_tasks),
    'invalid_attempt_counts', (SELECT value FROM invalid_attempt_counts),
    'invalid_running_attempts', (SELECT value FROM invalid_running_attempts)
);
