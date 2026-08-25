# TaskForge

TaskForge is a service-oriented task orchestration platform. The current milestone includes task submission and a deliberately small PostgreSQL-polling worker for predefined test handlers. Scheduling, retries, leases, priorities, Redis dispatch, and arbitrary task execution are not implemented.

## Services

| Service | Technology | Local port | Responsibility |
| --- | --- | ---: | --- |
| `api` | Python / FastAPI | 8000 | Public task API and task lifecycle ownership |
| `scheduler` | Go | 8081 | Future schedule evaluation and dispatch coordination |
| `worker` | Go | 8082 | Polls PostgreSQL and executes registered test handlers |
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
    make migrate-up    # Apply the PostgreSQL schema
    make migrate-down  # Roll back the PostgreSQL schema
    make down          # Stop the local stack

See [docs/architecture.md](docs/architecture.md) for service boundaries and communication paths.
