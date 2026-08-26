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
- `000005_expired_lease_recovery.up.sql` adds attempt-level `ABANDONED` semantics and protects task rows from using that status.
- `000005_expired_lease_recovery.down.sql` converts abandoned history to failed history and restores pre-TF-008 constraints. PostgreSQL retains the additive enum label until the foundational migration drops the enum; constraints prevent its use after this partial rollback.
- `000006_task_retries.up.sql` adds the partial due-retry promotion index.
- `000006_task_retries.down.sql` removes that index.
- `000007_submission_idempotency.up.sql` adds request fingerprints and globally unique durable submission keys.
- `000007_submission_idempotency.down.sql` removes fingerprint metadata and restores the earlier queue-scoped key index.

Apply or roll back the current migration against the Compose PostgreSQL service:

    make migrate-up
    make migrate-down

The migration runner waits for PostgreSQL, obtains a session advisory lock, and applies files in deterministic filename order. Successfully committed versions are stored in `schema_migrations`, so repeated upgrades are safe no-ops and parallel runners serialize. Each SQL file records or removes its own version inside the same transaction as its schema changes. The runner enables `ON_ERROR_STOP`, so execution stops at the first failure.

For a database created before `schema_migrations` existed, the runner baselines the complete foundational schema and the transactional `000002` claim marker before applying newer files. It refuses to infer a version from a partial foundational schema.

`make migrate-down` rolls back every currently applied migration in reverse order. A later `make migrate-up` recreates the schema from the remaining version state.

Run the PostgreSQL-backed migration test with:

    make test-migrations

The test creates a uniquely named schema, applies all migrations in order, verifies objects and constraints, rolls them back in reverse order, and removes the temporary schema.
