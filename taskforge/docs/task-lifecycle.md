# Task Lifecycle

PostgreSQL is authoritative for task and attempt state. `ABANDONED` is permitted only for attempt rows; `tasks_status_allowed` excludes it from tasks.

| Task status | Meaning |
| --- | --- |
| `QUEUED` | Eligible for claim when `scheduled_at` is due. |
| `LEASED` | Reserved and unused. |
| `RUNNING` | Owned by one worker attempt with a renewable lease. |
| `RETRYING` | A retryable handler failure is waiting until `scheduled_at`. |
| `SUCCEEDED` | Handler completed successfully; terminal. |
| `FAILED` | Terminal handler failure or exhausted attempt budget; terminal. |
| `CANCELLED` | Cancelled while unowned in `QUEUED` or `RETRYING`; terminal. |

Attempts use `RUNNING`, `SUCCEEDED`, `FAILED`, or `ABANDONED` in current execution paths. A `FAILED` attempt means the handler returned an error. `ABANDONED` means ownership expired before TaskForge accepted a valid handler result.

## Implemented transitions

    QUEUED
      | claim creates attempt N
      v
    RUNNING
      |-- success ------------------------------> SUCCEEDED
      |-- terminal handler error ---------------> FAILED
      |-- retryable handler error
      |      | attempt N -> FAILED
      |      | N < max_attempts
      |      v
      |   RETRYING
      |      | scheduler promotes when due
      |      v
      |   QUEUED
      |      `-- next claim creates attempt N+1
      `-- lease expiration
             | attempt N -> ABANDONED
             | N < max_attempts -> QUEUED
             ` N = max_attempts -> FAILED

Every claim consumes exactly one attempt regardless of whether it ends `FAILED`, `ABANDONED`, or `SUCCEEDED`. Neither retry scheduling, retry promotion, nor crash recovery creates the next attempt.

## Handler error classification

Handlers return success, a typed `RetryableError`, or an ordinary terminal error. Error strings are never parsed. Unknown task types and malformed payloads remain terminal. Attempt rows preserve the original error and timestamps.

## Retry timing

For zero-based `retry_index = attempt_number - 1`:

    exponential = min(max_delay, base_delay * 2^retry_index)
    multiplier  = 1 - jitter + (2 * jitter * random_unit)
    retry_delay = min(max_delay, exponential * multiplier)

`random_unit` is bounded to `[0,1]`; jitter is in `[0,1)`. Doubling is overflow-safe. The repository sets `scheduled_at = clock_timestamp() + retry_delay`, so PostgreSQL time is authoritative.

## Atomic retryable failure

The worker locks the task and requires its task ID, worker ID, attempt number, `RUNNING` state, and strictly valid lease. In one transaction it:

- marks attempt N `FAILED` with `finished_at` and the handler error;
- clears task owner and lease;
- preserves `attempt_count=N`;
- sets `RETRYING` and a future `scheduled_at` when attempts remain; or
- sets terminal `FAILED` and `completed_at` at exhaustion.

A stale worker receives `ErrLeaseLost` and cannot schedule a retry. A database failure after the attempt update rolls back both rows.

## Scheduler responsibilities

The scheduler runs independent loops:

- Expired lease recovery: `RUNNING` → `ABANDONED` attempt plus task requeue/failure.
- Retry promotion: due `RETRYING` → `QUEUED`.

Both use bounded `FOR UPDATE SKIP LOCKED` batches. Promotion changes no attempt row and creates no attempt.

## Cancellation

The API atomically permits `QUEUED|RETRYING -> CANCELLED`. It normalizes `scheduled_at`, sets `completed_at`, and leaves history unchanged. `RUNNING` cancellation remains a conflict because handler cancellation signaling is not implemented.

## Core invariants

- Only `RUNNING` tasks have owner and lease fields.
- A lease is valid only while `lease_expires_at > clock_timestamp()`.
- `RETRYING` requires `attempt_count < max_attempts`.
- Claim eligibility requires `QUEUED`, due `scheduled_at`, and attempts remaining.
- Attempt numbers are positive and unique per task.
- Terminal attempts have `finished_at`; active attempts do not.
- Terminal tasks have `completed_at`; waiting and running tasks do not.
