# Integration tests

`test_migrations.py` verifies the PostgreSQL schema against a real server. It covers:

- all task status enum labels;
- creation of `tasks`, `workers`, and `task_attempts`;
- required operational indexes;
- lease, terminal-state, foreign-key, idempotency, attempt-status, and uniqueness constraints;
- complete migration rollback.

Run it through Docker Compose:

    make test-migrations

The test operates inside a temporary schema and does not alter development tables.

