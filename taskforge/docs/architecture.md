# TaskForge Architecture

## Scope

This milestone implements task creation, the first worker execution path, and worker-liveness visibility. The worker supports only predefined test handlers, polls PostgreSQL directly, and records periodic heartbeats. Schedule evaluation, retries, leases, recovery, Redis dispatch, and arbitrary task execution remain out of scope.

## Service boundaries

### Web (`web`)

The React/TypeScript application is the browser-facing interface. It is built as static assets and served by Nginx. The web service must communicate with TaskForge only through the API; it does not connect directly to PostgreSQL, Redis, the scheduler, or workers.

### API (`api`)

The FastAPI service is the public application boundary. It owns request validation and task lifecycle mutations. PostgreSQL is the source of truth for durable state. Clients never depend on PostgreSQL or Redis directly.

The current API exposes `/healthz`, task submission, retrieval, listing, cancellation, and worker list/detail liveness, plus FastAPI's generated documentation.

### Scheduler (`scheduler`)

The Go scheduler will evaluate persisted schedules and coordinate dispatch of due work. It may read and update scheduling records in PostgreSQL and publish task identifiers to Redis. It must not execute user tasks or serve public product APIs.

The current scheduler starts an operational HTTP server with only `/healthz`; it does not inspect schedules or publish messages.

### Worker (`worker`)

The Go worker registers a process-lifetime instance identity in PostgreSQL, polls for eligible `QUEUED` tasks, atomically claims one task, records an attempt, executes a registered handler, and persists success or failure before polling again. Eligible work is ordered by priority descending, creation time ascending, then task ID ascending. `FOR UPDATE SKIP LOCKED` coordinates simultaneous claimers; ownership transition and attempt creation commit before handler execution begins. The controlled contention proof is documented in `docs/tf-005.md`.

The registry contains `test.echo`, `test.fail`, and a bounded `test.sleep` integration-test handler. Unknown task types fail safely; the worker never executes task-provided code or commands.

Each process keeps process liveness separate from per-task ownership:

    Worker process
      |-- process heartbeat loop
      `-- execution
          |-- handler
          `-- task lease renewal loop

Registration initializes the heartbeat using PostgreSQL time. The heartbeat loop periodically updates only its worker row. Each claimed task receives a database-time lease and a task-specific renewal goroutine while its handler runs. Completion requires the same worker, attempt, and an unexpired lease. Shutdown cancels these loops and waits before closing the database pool. Worker liveness is derived from heartbeat age; lease validity is derived independently from task ownership. Expired work remains `RUNNING` until a later recovery milestone.

### PostgreSQL (`postgres`)

PostgreSQL is the durable system of record for task definitions, lifecycle state, attempts, worker registrations, and results. Schema changes belong in `migrations/`.

### Redis (`redis`)

Redis is reserved for future transient coordination. It is not used by the API-to-worker execution path and remains independent of worker startup.

## Communication paths

    Browser
       |
       | HTTP/JSON
       v
    Web (static UI) -------- HTTP/JSON --------> API
                                                   |
                                                   | durable reads/writes
                                                   v
                                              PostgreSQL
                                                ^   ^
                          poll/claim/attempts ---+   +--- schedule queries (future)
                          results, leases,       |        Scheduler
                          and heartbeat          |
                                                |
                                             Worker

    Redis is present for future coordination but is not on this path.

All service-to-service traffic uses Docker Compose's internal DNS names (`api`, `postgres`, and `redis`). Only browser-facing development ports and health endpoints are published to the host.

## Health and startup

Each container has a health check:

- PostgreSQL uses `pg_isready`.
- Redis uses `redis-cli ping`.
- API, scheduler, worker, and web expose `/healthz`.

Compose waits for PostgreSQL before starting the API and worker, and waits for the API before starting the web container. The worker registers before starting its polling and heartbeat loops. Worker health endpoints remain internal to the Compose network so replicas do not compete for a host port. HTTP health endpoints report process health; control-plane worker liveness is derived separately from durable heartbeats.

## Ownership rules

- Public contracts live in the API and are documented in `docs/api.md`.
- Database evolution is ordered through `migrations/` and recorded in `schema_migrations`; services must not create ad hoc schemas at startup.
- The worker handler registry is the sole allowlist for executable task types.
- Scheduler and worker operational endpoints are not product APIs.
- Cross-service shared source packages should be avoided. Share explicit wire contracts instead, allowing Python, Go, and TypeScript services to evolve independently.

## Observability

Services currently log to standard output for collection by the container runtime. Future metrics, dashboards, and alerting configuration belong in `monitoring/`. Task IDs are included in worker execution logs; broader correlation IDs can be added when more communication paths exist.

## Repository layout

    api/          FastAPI service
    scheduler/    Go scheduler service
    worker/       Go worker service
    web/          React/TypeScript frontend
    migrations/   PostgreSQL migrations
    monitoring/   Metrics, dashboards, and alerting
    tests/        Cross-service and integration tests
    scripts/      Developer and operational scripts
    docs/         Architecture and contract documentation
