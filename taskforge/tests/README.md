# Integration tests

The Docker-backed suite uses a real PostgreSQL server and temporary schemas:

- `test_migrations.py` verifies schema creation, constraints, indexes, and rollback.
- `test_task_api.py` exercises task submission, retrieval, listing, cancellation, validation, conflicts, idempotency, and persisted `QUEUED` state through the FastAPI application.
- `test_worker_e2e.py` starts the compiled Go worker and proves API submission through `SUCCEEDED`, repeated polling, failure reporting, and deterministic priority ordering.
- `test_claim_races.py` releases API cancellation and the real Go claim transaction together, then verifies only valid serialized states.
- `test_worker_api.py` verifies worker list/detail liveness using PostgreSQL time.
- `test_worker_heartbeat.py` proves heartbeats continue during a long handler, stopped workers transition to stale/dead without task recovery, and multiple workers heartbeat independently.
- `worker/internal/repository/postgres_contention_test.go` uses Go channel barriers and independent pooled PostgreSQL connections for single-task, load, priority, rollback, lock-scope, and query-plan proofs.
- `worker/internal/repository/postgres_heartbeat_test.go` verifies durable registration and narrow heartbeat updates against PostgreSQL.

Run all integration tests:

    make test-integration

Run the contention suite with Go race detection:

    make test-claims

Run one integration area:

    make test-api
    make test-migrations
    make test-worker
    make test-heartbeats

Temporary schemas are removed after each test module, leaving development tables untouched.
