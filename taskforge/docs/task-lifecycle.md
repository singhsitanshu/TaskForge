# Task Lifecycle

PostgreSQL defines the task lifecycle with the `task_status` enum.

| Status | Meaning |
| --- | --- |
| `QUEUED` | Ready for immediate dispatch, or waiting for `scheduled_at`. |
| `LEASED` | Reserved by one worker for a bounded lease period. |
| `RUNNING` | Actively executing under the current lease. |
| `RETRYING` | A prior attempt failed and the task is waiting for another eligible dispatch time. |
| `SUCCEEDED` | Finished successfully; terminal. |
| `FAILED` | Exhausted retries or encountered a non-retryable failure; terminal. |
| `CANCELLED` | Stopped by user or system intent; terminal. |

## Database invariants

- `LEASED` and `RUNNING` tasks must have `leased_by_worker_id`, `lease_token`, and `lease_expires_at`. Other task states must not retain lease fields.
- `SUCCEEDED`, `FAILED`, and `CANCELLED` tasks must have `completed_at`. Non-terminal tasks must not.
- `RETRYING` requires at least one remaining attempt.
- Attempt numbers are positive and unique within a task.
- Attempts use only `LEASED`, `RUNNING`, `SUCCEEDED`, `FAILED`, and `CANCELLED`. `QUEUED` and `RETRYING` describe task-level waiting, not an individual attempt.
- Active attempts cannot have `finished_at`; terminal attempts must have it.

Application code must perform task and attempt transitions in one database transaction. Transition-policy enforcement beyond these shape constraints belongs in a later milestone with the task lifecycle implementation.

