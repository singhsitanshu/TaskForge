# Task API

The FastAPI service owns the public task contract. All endpoints use JSON and return persisted PostgreSQL state.

## Submit a task

`POST /tasks` accepts an optional, case-sensitive `Idempotency-Key` header. Without the header it creates a fresh `QUEUED` task and returns `201 Created`. With the header, the first semantic submission returns `201`; an identical replay returns the existing task with `200`, including after completion or cancellation. Same-key submissions with different semantics return `409` and error code `IDEMPOTENCY_KEY_REUSE`.

Keys are 1–255 characters from `A-Z`, `a-z`, `0-9`, `.`, `_`, `~`, `:`, `/`, `+`, `=`, and `-`. They are global because TaskForge is currently single-tenant. The deprecated JSON `idempotency_key` field remains accepted for backward compatibility; new clients should use the header, and header/body values must match if both are present.

Request fields:

| Field | Required | Notes |
| --- | --- | --- |
| `task_type` | yes | Non-empty task type, at most 255 characters. |
| `payload` | no | JSON object; defaults to an empty object. |
| `queue` | no | Queue name; defaults to `default`. |
| `priority` | no | Signed 16-bit value; higher values are claimed first. |
| `max_attempts` | no | Between 1 and 100; counts every claimed attempt, including failed and abandoned attempts. |
| `scheduled_at` | no | ISO 8601 timestamp; defaults to database time. |
| `idempotency_key` | no | Deprecated body alternative to the `Idempotency-Key` header. |

The fingerprint covers `task_type`, canonicalized `payload`, `priority`, `max_attempts`, `queue`, and optional `scheduled_at`. JSON object order does not matter; array order does. When a client loses a response, it should repeat the same request and key. New work needs a new key or no key.

## Get a task

`GET /tasks/{id}` returns the current persisted task or `404`.

## List tasks

`GET /tasks` returns an object with `items`, `limit`, and `offset`. Optional query parameters:

- `status`: one of the defined task statuses.
- `queue`: exact queue match.
- `limit`: 1–100, default 50.
- `offset`: non-negative, default 0.

Results are ordered by newest creation time, then ID.

## Cancel a task

`POST /tasks/{id}/cancel` atomically transitions `QUEUED` or `RETRYING` to `CANCELLED`, normalizes `scheduled_at`, and sets `completed_at`.

Cancellation is idempotent for an already `CANCELLED` task. Unknown tasks return `404`. `LEASED`, `RUNNING`, `SUCCEEDED`, and `FAILED` tasks return `409` without changing task or attempt state. Cancelling `RETRYING` changes no attempt history and prevents later scheduler promotion.

This endpoint changes lifecycle state only. It does not execute tasks or implement worker behavior.

Task responses expose `claimed_by_worker_id` and `lease_expires_at`. Both are non-null while a task has active or expired `RUNNING` ownership and are cleared on successful or failed completion. The API provides no lease-renewal endpoint; workers renew ownership directly through the repository protocol.

## List task attempts

`GET /tasks/{id}/attempts` returns `{ "items": [...] }` with durable attempt history ordered by `attempt_number` ascending. Unknown tasks return `404`; a task with no attempts returns an empty list.

Each item includes `id`, `task_id`, `worker_id`, `attempt_number`, `status`, `leased_at`, `started_at`, `finished_at`, `output`, `error`, `created_at`, and `updated_at`. `FAILED` records a handler error; `ABANDONED` identifies lease-loss recovery with `lease_expired`. Internal ownership secrets are not exposed.

Submission replay returns the original task, so this endpoint naturally returns that task's complete attempt history. Submission idempotency does not guarantee exactly-once handler execution or external side effects.

## List workers

`GET /workers` returns an object with `items`, `limit`, and `offset`. Workers are ordered by registration time and ID, newest first. Each item includes:

- `id`, `instance_id`, and human-readable `name`.
- Administrative `enabled` state and non-sensitive `metadata`.
- `registered_at`, `last_heartbeat`, and `updated_at` timestamps.
- Derived `liveness`: `ACTIVE`, `STALE`, or `DEAD`.
- `heartbeat_age_seconds`, calculated from the same PostgreSQL reference timestamp as liveness.

## Get a worker

`GET /workers/{worker_id}` returns the same worker representation or `404`. Liveness is derived when the request is served; it is not persisted as a status column.
