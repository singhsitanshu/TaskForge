# Worker

The first TaskForge worker is a deliberately small PostgreSQL polling loop:

1. Register or re-enable its process-lifetime worker instance ID and initialize `last_seen_at`.
2. Start an independent heartbeat loop.
3. Select one eligible `QUEUED` task by priority, creation time, and ID with `FOR UPDATE SKIP LOCKED`.
4. Atomically move it to `RUNNING` and create a task attempt.
5. Execute the matching allowlisted handler while heartbeats continue independently.
6. Atomically persist `SUCCEEDED` plus output, or `FAILED` plus an error.
7. Poll immediately after work, or wait `POLL_INTERVAL` when the queue is empty.

Supported handlers are `test.echo`, `test.fail`, and the bounded `test.sleep` integration-test handler. Unknown task types are recorded as failures and cannot execute arbitrary code.

Configuration:

- `DATABASE_URL` (required): PostgreSQL connection string.
- `WORKER_ID`: optional explicit process-lifetime instance ID. When absent, the worker derives one from hostname and a random process suffix.
- `WORKER_NAME`: optional human-readable name; defaults to the hostname and does not define instance identity.
- `POLL_INTERVAL`: positive Go duration; defaults to `1s`.
- `WORKER_HEARTBEAT_INTERVAL`: heartbeat period; defaults to `5s`.
- `WORKER_STALE_AFTER`: worker remains `ACTIVE` through this heartbeat age; defaults to `15s` and must exceed the heartbeat interval.
- `WORKER_DEAD_AFTER`: worker becomes `DEAD` after this heartbeat age; defaults to `30s` and must exceed the stale threshold.
- `WORKER_HEARTBEAT_TIMEOUT`: timeout for one heartbeat database update; defaults to `2s`.
- `HTTP_ADDR`: health server address; defaults to `:8080`.

Each Compose replica has a distinct hostname, so it registers a distinct instance and needs no published host health port. A restarted process may register a new instance identity unless `WORKER_ID` is explicitly supplied.

Registration and shutdown are logged at INFO with worker identity. Heartbeat failures log `event=heartbeat_failed`; successful periodic heartbeats are intentionally silent at INFO. Successful claims log `event=task_claimed`, `worker_instance_id`, `task_id`, and `attempt_number`. Polling misses are not logged.

This version detects liveness only. It does not implement retries, leases, dead-worker task recovery, or Redis dispatch. A dead worker can leave a task and attempt in `RUNNING`.
