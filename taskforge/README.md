# TaskForge

TaskForge is a service-oriented task orchestration platform. This repository currently contains the service shells, local infrastructure, and developer tooling only; task scheduling and execution are intentionally not implemented yet.

## Services

| Service | Technology | Local port | Responsibility |
| --- | --- | ---: | --- |
| `api` | Python / FastAPI | 8000 | Public HTTP API and future task lifecycle ownership |
| `scheduler` | Go | 8081 | Future schedule evaluation and dispatch coordination |
| `worker` | Go | 8082 | Future task execution; currently health checks only |
| `web` | React / TypeScript | 3000 | Browser interface |
| `postgres` | PostgreSQL | 5432 | Durable application state |
| `redis` | Redis | 6379 | Future queueing and short-lived coordination |

## Quick start

    cp .env.example .env
    make up
    make health

Open the web app at <http://localhost:3000> and the API documentation at <http://localhost:8000/docs>.

## Developer commands

    make lint          # Run static checks
    make format        # Apply formatters
    make format-check  # Verify formatting without modifying files
    make test          # Run service tests and build the frontend
    make migrate-up    # Apply the PostgreSQL schema
    make migrate-down  # Roll back the PostgreSQL schema
    make down          # Stop the local stack

See [docs/architecture.md](docs/architecture.md) for service boundaries and communication paths.
