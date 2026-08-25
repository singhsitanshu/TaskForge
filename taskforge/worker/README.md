# Worker

The first TaskForge worker is a deliberately small PostgreSQL polling loop:

1. Register or re-enable its process-lifetime worker instance ID.
2. Select one eligible `QUEUED` task by priority, creation time, and ID with `FOR UPDATE SKIP LOCKED`.
3. Atomically move it to `RUNNING` and create a task attempt.
4. Execute the matching allowlisted handler.
5. Atomically persist `SUCCEEDED` plus output, or `FAILED` plus an error.
6. Poll immediately after work, or wait `POLL_INTERVAL` when the queue is empty.

Supported handlers are `test.echo` and `test.fail`. Unknown task types are recorded as failures and cannot execute arbitrary code.

Configuration:

- `DATABASE_URL` (required): PostgreSQL connection string.
- `WORKER_ID`: optional explicit process-lifetime instance ID. When absent, the worker derives one from hostname and a random process suffix.
- `WORKER_NAME`: optional human-readable name; defaults to the hostname and does not define instance identity.
- `POLL_INTERVAL`: positive Go duration; defaults to `1s`.
- `HTTP_ADDR`: health server address; defaults to `:8080`.

Each Compose replica has a distinct hostname, so it registers a distinct instance and needs no published host health port. A restarted process may register a new instance identity unless `WORKER_ID` is explicitly supplied.

This version does not implement retries, leases, Redis, or heartbeats.
