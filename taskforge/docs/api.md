# Task API

The FastAPI service owns the public task contract. All endpoints use JSON and return persisted PostgreSQL state.

## Submit a task

`POST /tasks` creates a task in `QUEUED` status and returns `201 Created`.

Request fields:

| Field | Required | Notes |
| --- | --- | --- |
| `task_type` | yes | Non-empty task type, at most 255 characters. |
| `payload` | no | JSON object; defaults to an empty object. |
| `queue` | no | Queue name; defaults to `default`. |
| `priority` | no | Signed 16-bit value; higher values are claimed first. |
| `max_attempts` | no | Between 1 and 100; persisted, but retries are not implemented. |
| `scheduled_at` | no | ISO 8601 timestamp; defaults to database time. |
| `idempotency_key` | no | Unique within a queue. Duplicate submissions return `409`. |

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

`POST /tasks/{id}/cancel` atomically transitions only `QUEUED` to `CANCELLED` and sets `completed_at`.

Cancellation is idempotent for an already `CANCELLED` task. Unknown tasks return `404`. `LEASED`, `RUNNING`, `RETRYING`, `SUCCEEDED`, and `FAILED` tasks return `409` without changing task or attempt state.

This endpoint changes lifecycle state only. It does not execute tasks or implement worker behavior.

## List workers

`GET /workers` returns an object with `items`, `limit`, and `offset`. Workers are ordered by registration time and ID, newest first. Each item includes:

- `id`, `instance_id`, and human-readable `name`.
- Administrative `enabled` state and non-sensitive `metadata`.
- `registered_at`, `last_heartbeat`, and `updated_at` timestamps.
- Derived `liveness`: `ACTIVE`, `STALE`, or `DEAD`.
- `heartbeat_age_seconds`, calculated from the same PostgreSQL reference timestamp as liveness.

## Get a worker

`GET /workers/{worker_id}` returns the same worker representation or `404`. Liveness is derived when the request is served; it is not persisted as a status column.
