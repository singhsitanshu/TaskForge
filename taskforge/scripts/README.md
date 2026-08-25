# Scripts

`migrate.sh` is the raw-SQL migration runner used by `make migrate-up` and `make migrate-down`. It waits for Compose PostgreSQL readiness, serializes runners with an advisory lock, and consults the transactional `schema_migrations` ledger before applying or rolling back files.
