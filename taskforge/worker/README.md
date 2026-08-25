# Worker

The first TaskForge worker is a deliberately small PostgreSQL polling loop:

1. Register or re-enable its process-lifetime worker instance ID and initialize `last_seen_at`.
2. Start an independent process-heartbeat loop.
3. Select one eligible `QUEUED` task by priority, creation time, and ID with `FOR UPDATE SKIP LOCKED`.
4. Atomically move it to `RUNNING`, establish a database-time lease, and create a task attempt.
5. Execute the matching allowlisted handler alongside a task-specific lease-renewal loop.
6. Atomically persist success, terminal failure, or a typed retryable failure only while ownership and lease remain valid.
7. Poll immediately after work, or wait `POLL_INTERVAL` when the queue is empty.

Supported production behavior is restricted to registered handlers. Controlled retry handlers are included for integration testing. Unknown task types and malformed payloads are terminal failures and cannot execute arbitrary code.

Configuration:

- `DATABASE_URL` (required): PostgreSQL connection string.
- `WORKER_ID`: optional explicit process-lifetime instance ID. When absent, the worker derives one from hostname and a random process suffix.
- `WORKER_NAME`: optional human-readable name; defaults to the hostname and does not define instance identity.
- `POLL_INTERVAL`: positive Go duration; defaults to `1s`.
- `WORKER_HEARTBEAT_INTERVAL`: heartbeat period; defaults to `5s`.
- `WORKER_STALE_AFTER`: worker remains `ACTIVE` through this heartbeat age; defaults to `15s` and must exceed the heartbeat interval.
- `WORKER_DEAD_AFTER`: worker becomes `DEAD` after this heartbeat age; defaults to `30s` and must exceed the stale threshold.
- `WORKER_HEARTBEAT_TIMEOUT`: timeout for one heartbeat database update; defaults to `2s`.
- `WORKER_TASK_LEASE_DURATION`: task ownership duration; defaults to `30s`.
- `WORKER_TASK_LEASE_RENEW_INTERVAL`: renewal period; defaults to `10s` and must not exceed half the lease duration.
- `WORKER_TASK_LEASE_RENEW_TIMEOUT`: timeout for one renewal operation; defaults to `2s` and must be shorter than the lease duration.
- `TASK_RETRY_BASE_DELAY`: first retry delay; defaults to `2s`.
- `TASK_RETRY_MAX_DELAY`: hard backoff cap; defaults to `5m` and must be at least the base delay.
- `TASK_RETRY_JITTER`: bounded jitter fraction; defaults to `0.2` and must be in `[0,1)`.
- `HTTP_ADDR`: health server address; defaults to `:8080`.

Each Compose replica has a distinct hostname, so it registers a distinct instance and needs no published host health port. A restarted process may register a new instance identity unless `WORKER_ID` is explicitly supplied.

Registration and shutdown are logged at INFO with worker identity. Heartbeat failures log `event=heartbeat_failed`; successful periodic heartbeats are intentionally silent at INFO. Claims and lease starts log at INFO, successful renewals use `event=task_lease_renewed` at DEBUG, and renewal failure, lease loss, and stale completion rejection have structured warning events. Polling misses are not logged.

Retryable handler failures create `FAILED` attempts and `RETRYING` tasks without creating the next attempt. The scheduler promotes due retries. Expired ownership remains a separate scheduler recovery path that records `ABANDONED`. Redis dispatch remains unimplemented.
