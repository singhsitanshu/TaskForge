# Integration tests

The Docker-backed suite uses a real PostgreSQL server and temporary schemas:

- `test_migrations.py` verifies schema creation, constraints, indexes, and rollback.
- `test_task_api.py` exercises task submission, retrieval, listing, cancellation, validation, conflicts, idempotency, and persisted `QUEUED` state through the FastAPI application.
- `test_worker_e2e.py` starts the compiled Go worker and proves API submission through `SUCCEEDED`, repeated polling, and failure reporting.

Run all integration tests:

    make test-integration

Run one integration area:

    make test-api
    make test-migrations
    make test-worker

Temporary schemas are removed after each test module, leaving development tables untouched.
