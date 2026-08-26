# Integration tests

The Docker-backed suite uses a real PostgreSQL server and temporary schemas:

- `test_migrations.py` verifies schema creation, constraints, indexes, and rollback.
- `test_task_api.py` exercises task submission, retrieval, listing, cancellation, validation, conflicts, idempotency, and persisted `QUEUED` state through the FastAPI application.
- `test_idempotency_api.py` proves canonical replay, semantic conflicts, lifecycle independence, keyless behavior, rollback, index use, repeated 100-way duplicate races, and mixed-fingerprint contention against PostgreSQL.
- `test_worker_e2e.py` starts the compiled Go worker and proves API submission through `SUCCEEDED`, repeated polling, failure reporting, deterministic priority ordering, readiness, and live Prometheus counter/histogram output.
- `test_claim_races.py` releases API cancellation and the real Go claim transaction together, then verifies only valid serialized states.
- `test_worker_api.py` verifies worker list/detail liveness using PostgreSQL time.
- `test_worker_heartbeat.py` proves heartbeats and leases advance during a long handler, lease loss cannot complete stale work, hard crashes leave expired `RUNNING` ownership, and multiple workers heartbeat independently.
- `test_recovery_e2e.py` proves hard-crash recovery, delayed retry success/exhaustion, mixed failed/abandoned histories, and ordered API history.
- `worker/internal/repository/postgres_contention_test.go` uses Go channel barriers and independent pooled PostgreSQL connections for single-task, load, priority, rollback, lock-scope, and query-plan proofs.
- `worker/internal/repository/postgres_heartbeat_test.go` verifies durable registration and narrow heartbeat updates against PostgreSQL.
- `worker/internal/repository/postgres_lease_test.go` verifies claim leases, renewal ownership, stale completion rejection, terminal cleanup, and the expired-lease query plan.
- `scheduler/internal/repository/postgres_recovery_test.go` verifies recovery contention, batches, valid-lease isolation, boundaries, max attempts, rollback, corruption handling, liveness independence, replica safety, stale-owner fencing, and the recovery query plan.
- `worker/internal/repository/postgres_retry_test.go` verifies atomic retry scheduling, exhaustion, stale-owner rejection, and rollback.
- `scheduler/internal/repository/postgres_promotion_test.go` verifies due boundaries, 500-row promotion contention, no attempt creation, and index use.
- `scheduler/internal/repository/postgres_metrics_test.go` seeds exact task/attempt/liveness populations and verifies PostgreSQL aggregate snapshots replace rather than accumulate.
- `test_monitoring_config.py` validates scalable Prometheus discovery, automatic Grafana provisioning, dashboard sections, and TaskForge metric naming.

Run all integration tests:

    make test-integration

Run the contention suite with Go race detection:

    make test-claims

Run the scheduler recovery suite with Go race detection:

    make test-recovery
    make test-retries
    make test-idempotency
    make test-observability

Run one integration area:

    make test-api
    make test-migrations
    make test-worker
    make test-heartbeats
    make test-leases

Temporary schemas are removed after each test module, leaving development tables untouched.
