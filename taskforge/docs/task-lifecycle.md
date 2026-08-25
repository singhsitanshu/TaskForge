# Task Lifecycle

PostgreSQL defines the task lifecycle with the `task_status` enum.

| Status | Meaning |
| --- | --- |
| `QUEUED` | Ready for immediate dispatch, or waiting for `scheduled_at`. |
| `LEASED` | Reserved for a future bounded-lease implementation; unused by the first worker. |
| `RUNNING` | Claimed and actively executing on one worker. |
| `RETRYING` | A prior attempt failed and the task is waiting for another eligible dispatch time. |
| `SUCCEEDED` | Finished successfully; terminal. |
| `FAILED` | Exhausted retries or encountered a non-retryable failure; terminal. |
| `CANCELLED` | Stopped by user or system intent; terminal. |

## First worker transitions

The worker implements only `QUEUED -> RUNNING -> SUCCEEDED|FAILED`. Claiming creates exactly one `RUNNING` attempt, stores `claimed_by_worker_id`, and establishes a task lease. Completion updates the task and attempt in one transaction and clears both claim and lease. The API permits only `QUEUED -> CANCELLED`; cancellation of any other state returns a conflict until worker cancellation signaling exists.

Retries and `RETRYING` transitions are not implemented. The worker does not create leases. Eligible queued tasks are claimed by priority descending, then creation time and task ID ascending.

## Database invariants

- A `RUNNING` task must have `claimed_by_worker_id` and `lease_expires_at`; every other status must clear both.
- Renewal and completion require the current worker, current attempt number, and `lease_expires_at > clock_timestamp()`. Equality is expired.
- An expired task remains `RUNNING` with its attempt `RUNNING` in TF-007; no recovery transition is implemented.
- `SUCCEEDED`, `FAILED`, and `CANCELLED` tasks must have `completed_at`. Non-terminal tasks must not.
- `RETRYING` requires at least one remaining attempt.
- Attempt numbers are positive and unique within a task.
- Attempts use only `LEASED`, `RUNNING`, `SUCCEEDED`, `FAILED`, and `CANCELLED`. `QUEUED` and `RETRYING` describe task-level waiting, not an individual attempt.
- Active attempts cannot have `finished_at`; terminal attempts must have it.

The worker performs each claim and each task/attempt completion atomically in a database transaction. Additional transition policy will be added with later lifecycle features.
