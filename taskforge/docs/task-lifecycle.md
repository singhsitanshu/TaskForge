# Task Lifecycle

PostgreSQL defines task-level states with `task_status`. The shared enum also contains `ABANDONED`, but the `tasks_status_allowed` constraint reserves that state for attempt rows only.

| Task status | Meaning |
| --- | --- |
| `QUEUED` | Eligible for claim when `scheduled_at` is due. |
| `LEASED` | Reserved and currently unused. |
| `RUNNING` | Owned by one worker attempt, with a renewable task lease. |
| `RETRYING` | Reserved for future ordinary retry policy; currently unused. |
| `SUCCEEDED` | Handler completed successfully; terminal. |
| `FAILED` | Handler failed, or crash recovery exhausted `max_attempts`; terminal. |
| `CANCELLED` | Cancelled while queued; terminal. |

Attempt rows use `LEASED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, and `ABANDONED`. `ABANDONED` means the attempt lost authority because its task lease expired; it is terminal, has `finished_at`, and carries the stable `lease_expired` reason. It does not mean the handler returned an error.

## Implemented transitions

    QUEUED
      | claim: owner + lease, attempt N
      v
    RUNNING
      |-- handler success ----------------------> SUCCEEDED
      |-- ordinary handler failure -------------> FAILED
      `-- lease_expires_at <= database time
            | scheduler transaction:
            | attempt N -> ABANDONED
            |
            |-- N < max_attempts -> QUEUED
            |                        `-- next claim creates attempt N+1
            `-- N = max_attempts -> FAILED

The API additionally implements `QUEUED -> CANCELLED`. It rejects cancellation once a task is running or terminal because running-task cancellation signaling is not implemented.

Ordinary handler failures are not retried. `RETRYING`, backoff, and retry scheduling remain separate future behavior.

## Recovery transaction

For each expired locked task, the scheduler validates that the unique attempt at `(task_id, task.attempt_count)`:

- exists;
- belongs to `tasks.claimed_by_worker_id`;
- has the same attempt number;
- is `RUNNING`.

It then marks that attempt `ABANDONED`, sets `finished_at = clock_timestamp()` and `error = lease_expired`, and transitions the task in the same transaction:

- Remaining attempt capacity: `QUEUED`, ownership and lease cleared, `attempt_count` preserved, `last_error = lease_expired`.
- No remaining capacity: `FAILED`, ownership and lease cleared, `completed_at = clock_timestamp()`, `last_error = max_attempts_exhausted_after_lease_expiration`.

Recovery creates no attempt. Claiming the requeued task increments `attempt_count` and inserts the next attempt. Any task-update failure rolls back the preceding attempt update.

## Database invariants

- `RUNNING` requires `claimed_by_worker_id` and `lease_expires_at`; every other task state clears both.
- Lease validity is strict: `lease_expires_at > clock_timestamp()`. Equality is expired for renewal, completion, and recovery.
- Terminal tasks require `completed_at`; non-terminal tasks forbid it.
- `attempt_count` stays between zero and `max_attempts`.
- Worker claim eligibility includes `attempt_count < max_attempts`.
- Attempt numbers are positive and unique per task.
- `RUNNING` attempts have `started_at` and no `finished_at`.
- Terminal attempts, including `ABANDONED`, have `finished_at`.
- A missing or mismatched active attempt blocks recovery and produces an invariant-violation log; durable state remains unchanged.

## Heartbeat versus lease

Worker heartbeat describes whether a process appears `ACTIVE`, `STALE`, or `DEAD`. The task lease alone authorizes work. Therefore an `ACTIVE` worker with an expired lease is recoverable, while a `DEAD` worker with a still-valid lease is not recovered until lease expiration.
