# TaskForge Architecture

## Scope

This milestone establishes deployable service boundaries and local infrastructure. It does **not** implement task creation, schedule evaluation, queue publishing, queue consumption, or task execution. The PostgreSQL and Redis connections documented below describe the intended paths for later milestones.

## Service boundaries

### Web (`web`)

The React/TypeScript application is the browser-facing interface. It is built as static assets and served by Nginx. The web service must communicate with TaskForge only through the API; it does not connect directly to PostgreSQL, Redis, the scheduler, or workers.

### API (`api`)

The FastAPI service is the public application boundary. It will own request validation, authentication and authorization, and task lifecycle mutations. PostgreSQL is the source of truth for durable state. When dispatch is implemented, the API may publish immediate work to Redis, but clients should never depend on Redis directly.

The current API exposes only `/healthz` and FastAPI's generated documentation.

### Scheduler (`scheduler`)

The Go scheduler will evaluate persisted schedules and coordinate dispatch of due work. It may read and update scheduling records in PostgreSQL and publish task identifiers to Redis. It must not execute user tasks or serve public product APIs.

The current scheduler starts an operational HTTP server with only `/healthz`; it does not inspect schedules or publish messages.

### Worker (`worker`)

The Go worker will consume task identifiers from Redis, load authoritative task data, execute supported task types, and persist results. It must not accept public task submissions or decide when scheduled work becomes due.

The current worker starts an operational HTTP server with only `/healthz`; it does not consume messages or execute tasks.

### PostgreSQL (`postgres`)

PostgreSQL is the durable system of record for task definitions, schedules, lifecycle state, attempts, and results. Schema changes belong in `migrations/`. No application schema is introduced in this milestone.

### Redis (`redis`)

Redis is reserved for transient coordination: dispatch queues, delivery metadata, and short-lived locks. It is not the source of truth. Queue messages should eventually contain stable identifiers rather than full authoritative task records so consumers can reconcile with PostgreSQL.

## Communication paths

    Browser
       |
       | HTTP/JSON
       v
    Web (static UI) -------- HTTP/JSON --------> API
                                                   |
                                                   | durable reads/writes (future)
                                                   v
                                              PostgreSQL
                                                   ^
                                                   |
    Scheduler -------- schedule queries (future) --+
       |
       | enqueue task ID (future)
       v
     Redis queue -------- dequeue task ID (future) --------> Worker
                                                              |
                                                              | lifecycle/result writes (future)
                                                              v
                                                         PostgreSQL

All service-to-service traffic uses Docker Compose's internal DNS names (`api`, `postgres`, and `redis`). Only browser-facing development ports and health endpoints are published to the host.

## Health and startup

Each container has a health check:

- PostgreSQL uses `pg_isready`.
- Redis uses `redis-cli ping`.
- API, scheduler, worker, and web expose `/healthz`.

Compose waits for PostgreSQL and Redis before starting backend service shells, and waits for the API before starting the web container. The HTTP health endpoints are liveness checks; they deliberately do not claim that unimplemented task functionality is ready.

## Ownership rules

- Public contracts live in the API and are documented in `docs/api.md`.
- Database evolution is append-only through `migrations/`; services must not create ad hoc schemas at startup.
- Redis payloads are internal versioned contracts between producers and workers.
- Scheduler and worker operational endpoints are not product APIs.
- Cross-service shared source packages should be avoided. Share explicit wire contracts instead, allowing Python, Go, and TypeScript services to evolve independently.

## Observability

Services currently log to standard output for collection by the container runtime. Future metrics, dashboards, and alerting configuration belong in `monitoring/`. Correlation IDs and task IDs should be carried across API requests, queue messages, scheduler logs, and worker logs once those paths exist.

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

