# Migrations

TaskForge uses paired, ordered PostgreSQL migration files:

- `000001_tasks_workers_attempts.up.sql` creates the initial task lifecycle schema.
- `000001_tasks_workers_attempts.down.sql` completely rolls it back.
- `000002_first_worker_claim.up.sql` adds the no-lease worker claim shape.
- `000002_first_worker_claim.down.sql` restores the initial lease-shaped schema.
- `000003_pre_tf005_remediation.up.sql` adds worker instance identity and the priority claim index.
- `000003_pre_tf005_remediation.down.sql` removes those remediation objects.
- `000004_task_leases.up.sql` canonicalizes task leases and adds the expired-running-task index.
- `000004_task_leases.down.sql` restores the pre-TF-007 lease scaffolding.

Apply or roll back the current migration against the Compose PostgreSQL service:

    make migrate-up
    make migrate-down

The migration runner waits for PostgreSQL, obtains a session advisory lock, and applies files in deterministic filename order. Successfully committed versions are stored in `schema_migrations`, so repeated upgrades are safe no-ops and parallel runners serialize. Each SQL file records or removes its own version inside the same transaction as its schema changes. The runner enables `ON_ERROR_STOP`, so execution stops at the first failure.

For a database created before `schema_migrations` existed, the runner baselines the complete foundational schema and the transactional `000002` claim marker before applying newer files. It refuses to infer a version from a partial foundational schema.

`make migrate-down` rolls back every currently applied migration in reverse order. A later `make migrate-up` recreates the schema from the remaining version state.

Run the PostgreSQL-backed migration test with:

    make test-migrations

The test creates a uniquely named schema, applies all migrations in order, verifies objects and constraints, rolls them back in reverse order, and removes the temporary schema.
