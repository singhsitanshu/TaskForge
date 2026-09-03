# Scripts

`migrate.sh` is the raw-SQL migration runner used by `make migrate-up` and `make migrate-down`. It waits for Compose PostgreSQL readiness, serializes runners with an advisory lock, and consults the transactional `schema_migrations` ledger before applying or rolling back files.

`demo/` contains the dependency-free Python client behind the `make demo*` targets. It submits only
through the public API, uses bounded polling, validates durable attempt histories, and restricts its
worker failure injection to the local Compose project. See [`docs/demo.md`](../docs/demo.md).
