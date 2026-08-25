# Worker

The first TaskForge worker is a deliberately small PostgreSQL polling loop:

1. Register or re-enable its configured worker name.
2. Select one eligible `QUEUED` task with `FOR UPDATE SKIP LOCKED`.
3. Atomically move it to `RUNNING` and create a task attempt.
4. Execute the matching allowlisted handler.
5. Atomically persist `SUCCEEDED` plus output, or `FAILED` plus an error.
6. Poll immediately after work, or wait `POLL_INTERVAL` when the queue is empty.

Supported handlers are `test.echo` and `test.fail`. Unknown task types are recorded as failures and cannot execute arbitrary code.

Configuration:

- `DATABASE_URL` (required): PostgreSQL connection string.
- `WORKER_NAME`: stable registration name; defaults to the hostname.
- `POLL_INTERVAL`: positive Go duration; defaults to `1s`.
- `HTTP_ADDR`: health server address; defaults to `:8080`.

This version does not implement retries, leases, Redis, priority ordering, or heartbeats.
