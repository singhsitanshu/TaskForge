# Migrations

TaskForge uses paired, ordered PostgreSQL migration files:

- `000001_tasks_workers_attempts.up.sql` creates the initial task lifecycle schema.
- `000001_tasks_workers_attempts.down.sql` completely rolls it back.
- `000002_first_worker_claim.up.sql` adds the no-lease worker claim shape.
- `000002_first_worker_claim.down.sql` restores the initial lease-shaped schema.

Apply or roll back the current migration against the Compose PostgreSQL service:

    make migrate-up
    make migrate-down

Both commands enable `ON_ERROR_STOP`. The SQL files also wrap their changes in a transaction, so a failed migration does not leave a partially applied schema.

Run the PostgreSQL-backed migration test with:

    make test-migrations

The test creates a uniquely named schema, applies all migrations in order, verifies objects and constraints, rolls them back in reverse order, and removes the temporary schema.
