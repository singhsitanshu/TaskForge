# TaskForge

TaskForge is a service-oriented task orchestration platform. The current milestone includes durable idempotent task submission, priority-aware PostgreSQL workers, durable worker heartbeats, renewable leases, expired-ownership recovery, and typed application retries with delayed exponential backoff. Execution remains at-least-once; submission idempotency does not make arbitrary external side effects exactly-once. Recurring schedules, Redis dispatch, and arbitrary task execution are not implemented.

## Services

| Service | Technology | Local port | Responsibility |
| --- | --- | ---: | --- |
| `api` | Python / FastAPI | 8000 | Task control plane and worker-liveness visibility |
| `scheduler` | Go | internal | Recovers expired leases and promotes due retries transactionally |
| `worker` | Go | internal | Polls PostgreSQL, executes registered handlers, and heartbeats independently |
| `web` | React / TypeScript | 3000 | Browser interface |
| `postgres` | PostgreSQL | 5432 | Durable application state |
| `redis` | Redis | 6379 | Future queueing and short-lived coordination |
| `prometheus` | Prometheus | 9090 | Scrapes all API, worker, and scheduler replicas |
| `grafana` | Grafana | 3001 | Provisioned TaskForge operational dashboard |

## Quick start

    cp .env.example .env
    make migrate-up
    make up
    make health

Open the web app at <http://localhost:3000>, API documentation at <http://localhost:8000/docs>, Prometheus at <http://localhost:9090>, and Grafana at <http://localhost:3001>.

## Developer commands

    make lint          # Run static checks
    make format        # Apply formatters
    make format-check  # Verify formatting without modifying files
    make test          # Run service tests and build the frontend
    make test-worker   # Run the PostgreSQL-backed worker end-to-end test
    make test-heartbeats # Run worker-liveness integration tests
    make test-recovery  # Run scheduler recovery tests with Go race detection
    make test-retries   # Run retry scheduling, promotion, and E2E proofs
    make test-idempotency # Run submission replay and contention proofs
    make test-observability # Run metrics, readiness, collector, and monitoring tests
    make migrate-up    # Apply the PostgreSQL schema
    make migrate-down  # Roll back the PostgreSQL schema
    make down          # Stop the local stack

See [docs/architecture.md](docs/architecture.md) for service boundaries and communication paths.
See [docs/tf-010.md](docs/tf-010.md) for the idempotent-submission contract and execution guarantee boundary.
See [docs/tf-011.md](docs/tf-011.md) for metrics, cardinality, latency, and health/readiness contracts.
