# TaskForge

TaskForge is a service-oriented task orchestration platform. The current milestone includes task submission, priority-aware PostgreSQL workers, durable worker heartbeats, renewable leases, expired-ownership recovery, and typed application retries with delayed exponential backoff. Recurring schedules, Redis dispatch, and arbitrary task execution are not implemented.

## Services

| Service | Technology | Local port | Responsibility |
| --- | --- | ---: | --- |
| `api` | Python / FastAPI | 8000 | Task control plane and worker-liveness visibility |
| `scheduler` | Go | internal | Recovers expired leases and promotes due retries transactionally |
| `worker` | Go | internal | Polls PostgreSQL, executes registered handlers, and heartbeats independently |
| `web` | React / TypeScript | 3000 | Browser interface |
| `postgres` | PostgreSQL | 5432 | Durable application state |
| `redis` | Redis | 6379 | Future queueing and short-lived coordination |

## Quick start

    cp .env.example .env
    make migrate-up
    make up
    make health

Open the web app at <http://localhost:3000> and the API documentation at <http://localhost:8000/docs>.

## Developer commands

    make lint          # Run static checks
    make format        # Apply formatters
    make format-check  # Verify formatting without modifying files
    make test          # Run service tests and build the frontend
    make test-worker   # Run the PostgreSQL-backed worker end-to-end test
    make test-heartbeats # Run worker-liveness integration tests
    make test-recovery  # Run scheduler recovery tests with Go race detection
    make test-retries   # Run retry scheduling, promotion, and E2E proofs
    make migrate-up    # Apply the PostgreSQL schema
    make migrate-down  # Roll back the PostgreSQL schema
    make down          # Stop the local stack

See [docs/architecture.md](docs/architecture.md) for service boundaries and communication paths.
