# TaskForge Architecture

## Scope

TaskForge currently provides task submission, predefined Go worker handlers, durable worker heartbeats, renewable task leases, expired-ownership recovery, and typed handler retries with delayed exponential backoff. PostgreSQL is authoritative for every lifecycle transition. Recurring schedule evaluation, Redis dispatch, and arbitrary task execution remain out of scope.

## Service boundaries

### Web (`web`)

The React/TypeScript application is the browser-facing interface. It is built as static assets and served by Nginx. It communicates only through the API and never connects directly to PostgreSQL, Redis, the scheduler, or workers.

### API (`api`)

FastAPI is the public control-plane boundary. It validates submissions, reads task and ordered attempt history, permits cancellation of unowned `QUEUED` and `RETRYING` work, and derives worker liveness for operators. It does not dispatch or execute work. PostgreSQL is the source of every API response.

### Scheduler (`scheduler`)

The Go scheduler owns two independent lifecycle-maintenance loops:

    Scheduler
    |-- Recovery Loop
        |-- find expired RUNNING tasks
        |-- lock each task with FOR UPDATE SKIP LOCKED
        |-- validate and mark its current attempt ABANDONED
    |   `-- requeue the task or fail it at max_attempts
    `-- Retry Promotion Loop
        |-- find due RETRYING tasks
        |-- lock with FOR UPDATE SKIP LOCKED
        `-- promote to QUEUED without creating attempts

Each scan has its own bounded batch, ticker, and database timeout. Any scheduler replica may run both; PostgreSQL row locking coordinates replicas without leader election. Recovery uses task leases, while retry promotion uses `scheduled_at`. Worker liveness affects neither.

The scheduler exposes only the internal operational `/healthz` endpoint. Schedule evaluation and Redis publication are future responsibilities.

### Worker (`worker`)

The Go worker registers a process-lifetime identity, heartbeats independently, polls eligible `QUEUED` rows, atomically claims one task, creates its next numbered attempt, runs an allowlisted handler, renews its lease, and atomically records success or failure. Claim order is priority descending, creation time ascending, then task ID ascending. `FOR UPDATE SKIP LOCKED` coordinates simultaneous workers.

Handlers receive minimal task/attempt/worker execution metadata and explicitly return typed retryable errors or ordinary terminal errors. Unknown task types and malformed payloads fail terminally; task payloads never become executable commands or code.

Each process separates liveness from ownership:

    Worker process
      |-- heartbeat loop ---------> workers.last_seen_at
      `-- task execution
          |-- handler
          `-- lease renewal ------> tasks.lease_expires_at

Renewal, completion, and retry scheduling require the same task, worker, attempt number, `RUNNING` status, and a strictly unexpired lease. Retryable failure atomically records a `FAILED` attempt and `RETRYING` task. Once recovery or retry transition clears ownership, the old worker cannot mutate it. Only a later claim creates attempt `N+1`.

### PostgreSQL (`postgres`)

PostgreSQL is the durable coordination mechanism and system of record for tasks, attempts, workers, ownership, results, retries, and recovery. Claim, completion, retry failure, promotion, and recovery use atomic database transactions.

### Redis (`redis`)

Redis is provisioned for future transient dispatch and coordination. It is not on the current execution or recovery path.

## Communication paths

    Browser --HTTP--> Web --HTTP/JSON--> API -----------+
                                                       |
                                                       v
                                                  PostgreSQL
                                                   ^   ^   ^
                                                   |   |   |
                         poll / claim / renew ------+   |   +------ recovery and
                         attempts / completion / retry  |           retry promotion
                                                       |                |
                                                    Worker          Scheduler

    Redis is present but unused.

Containers use Compose internal DNS. API and web development ports are host-published. Worker and scheduler health endpoints remain internal, so replicas do not compete for host ports.

## Scheduler concurrency and failure model

The expired-task query is ordered by lease expiration and uses the partial `tasks_running_lease_idx`. A scanner locks at most its configured batch with `FOR UPDATE SKIP LOCKED`. For every locked task it locks the attempt at `(task_id, attempt_count)` and verifies worker, number, and `RUNNING` status.

Attempt abandonment and task requeue/failure are in one transaction. Retry promotion separately uses `tasks_retry_due_idx` and atomically changes only due `RETRYING` rows to `QUEUED`. Neither maintenance action creates an attempt. Transient failures are retried on the appropriate next interval without a busy loop.

## Health and startup

- PostgreSQL uses `pg_isready`.
- Redis uses `redis-cli ping`.
- API, scheduler, worker, and web expose `/healthz`.
- API, scheduler, and worker start after PostgreSQL is healthy.
- Web starts after API is healthy.

HTTP health reports process availability. Worker `ACTIVE`/`STALE`/`DEAD` is separately derived from durable heartbeats.

## Ownership rules

- Public contracts live in the API and `docs/api.md`.
- Ordered migrations and `schema_migrations` own database evolution.
- The worker registry is the only executable-task allowlist.
- Scheduler and worker health endpoints are operational, not product APIs.
- Services share durable or wire contracts, not cross-language source packages.

## Observability

Services log to standard output. Retry scheduling, exhaustion, promotion, recovery, and invariant errors use structured event fields. Empty scheduler scans are DEBUG-only. Metrics, dashboards, and alerting are intentionally deferred.

## Repository layout

    api/          FastAPI control plane
    scheduler/    Go lifecycle-maintenance service
    worker/       Go execution service
    web/          React/TypeScript frontend
    migrations/   PostgreSQL migrations
    monitoring/   Future metrics and dashboards
    tests/        Cross-service and integration tests
    scripts/      Developer and operational scripts
    docs/         Architecture and contract documentation
