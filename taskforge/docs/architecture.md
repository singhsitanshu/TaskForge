# TaskForge Architecture

## Scope

TaskForge currently provides task submission, predefined Go worker handlers, durable worker heartbeats, renewable task leases, and scheduler-driven recovery of expired ownership. PostgreSQL is authoritative for every lifecycle transition. Ordinary handler retries, retry backoff, schedule evaluation, Redis dispatch, and arbitrary task execution remain out of scope.

## Service boundaries

### Web (`web`)

The React/TypeScript application is the browser-facing interface. It is built as static assets and served by Nginx. It communicates only through the API and never connects directly to PostgreSQL, Redis, the scheduler, or workers.

### API (`api`)

FastAPI is the public control-plane boundary. It validates submissions, reads task and ordered attempt history, permits queued-task cancellation, and derives worker liveness for operators. It does not dispatch or execute work. PostgreSQL is the source of every API response.

### Scheduler (`scheduler`)

The Go scheduler owns system-driven lifecycle maintenance. Its TF-008 recovery loop:

    Scheduler
    `-- Recovery Loop
        |-- find expired RUNNING tasks
        |-- lock each task with FOR UPDATE SKIP LOCKED
        |-- validate and mark its current attempt ABANDONED
        `-- requeue the task or fail it at max_attempts

Each scan has a bounded batch and database timeout. Any scheduler replica may scan; PostgreSQL row locking coordinates replicas without leader election. Recovery uses only `tasks.lease_expires_at <= clock_timestamp()`. Worker heartbeat/liveness never grants or revokes task ownership.

The scheduler exposes only the internal operational `/healthz` endpoint. Schedule evaluation and Redis publication are future responsibilities.

### Worker (`worker`)

The Go worker registers a process-lifetime identity, heartbeats independently, polls eligible `QUEUED` rows, atomically claims one task, creates its next numbered attempt, runs an allowlisted handler, renews its lease, and atomically records success or failure. Claim order is priority descending, creation time ascending, then task ID ascending. `FOR UPDATE SKIP LOCKED` coordinates simultaneous workers.

The handler registry contains `test.echo`, `test.fail`, and bounded `test.sleep`. Unknown task types fail safely; task payloads never become executable commands or code.

Each process separates liveness from ownership:

    Worker process
      |-- heartbeat loop ---------> workers.last_seen_at
      `-- task execution
          |-- handler
          `-- lease renewal ------> tasks.lease_expires_at

Renewal and completion require the same task, worker, attempt number, `RUNNING` status, and a strictly unexpired lease. Once the scheduler recovers a task, the old worker cannot renew, succeed, or fail it. A later claim creates attempt `N+1`; recovery itself never creates an attempt.

### PostgreSQL (`postgres`)

PostgreSQL is the durable coordination mechanism and system of record for tasks, attempts, workers, ownership, results, and recovery. Claim, completion, and recovery each use explicit transactions. Migrations own all schema evolution.

### Redis (`redis`)

Redis is provisioned for future transient dispatch and coordination. It is not on the current execution or recovery path.

## Communication paths

    Browser --HTTP--> Web --HTTP/JSON--> API -----------+
                                                       |
                                                       v
                                                  PostgreSQL
                                                   ^   ^   ^
                                                   |   |   |
                         poll / claim / renew ------+   |   +------ expired-lease scan
                         attempts / completion         |           and recovery
                                                       |                |
                                                    Worker          Scheduler

    Redis is present but unused.

Containers use Compose internal DNS. API and web development ports are host-published. Worker and scheduler health endpoints remain internal, so replicas do not compete for host ports.

## Recovery concurrency and failure model

The expired-task query is ordered by lease expiration and uses the partial `tasks_running_lease_idx`. A scanner locks at most its configured batch with `FOR UPDATE SKIP LOCKED`. For every locked task it locks the attempt at `(task_id, attempt_count)` and verifies worker, number, and `RUNNING` status.

Attempt abandonment and task requeue/failure are in the same transaction. A database error rolls back both. A missing or mismatched attempt is reported as `task_recovery_invariant_violation` and left untouched; the scheduler never guesses at corrupted history. Transient scan failures are logged and retried at the next interval without a busy loop.

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

Services log to standard output. Scheduler recovery logs include the event, task, former worker, attempt number, lease expiration, and action. Empty scans are DEBUG-only. Metrics, dashboards, and alerting are intentionally deferred.

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
