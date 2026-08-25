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
| `priority` | no | Signed 16-bit value; persisted but ignored by the first worker. |
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

`POST /tasks/{id}/cancel` atomically transitions `QUEUED`, `LEASED`, `RUNNING`, or `RETRYING` to `CANCELLED`, sets `completed_at`, and clears worker ownership metadata.

Cancellation is idempotent for an already `CANCELLED` task. Unknown tasks return `404`; `SUCCEEDED` or `FAILED` tasks return `409`.

This endpoint changes lifecycle state only. It does not execute tasks or implement worker behavior.
