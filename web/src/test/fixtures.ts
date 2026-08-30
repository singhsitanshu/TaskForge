import type { Overview, Task, TaskAttempt, Worker } from "../types";

export const task: Task = {
  id: "11111111-1111-4111-8111-111111111111",
  queue: "default",
  task_type: "test.echo",
  payload: { message: "hello" },
  status: "SUCCEEDED",
  priority: 10,
  max_attempts: 3,
  attempt_count: 2,
  scheduled_at: "2026-08-30T10:00:00Z",
  queued_at: "2026-08-30T10:00:00Z",
  claimed_by_worker_id: null,
  lease_expires_at: null,
  completed_at: "2026-08-30T10:00:02Z",
  result: { echo: { message: "hello" } },
  last_error: "retryable failure",
  idempotency_key: "console-demo",
  created_at: "2026-08-30T10:00:00Z",
  updated_at: "2026-08-30T10:00:02Z",
};

export const failedAttempt: TaskAttempt = {
  id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  task_id: task.id,
  worker_id: "22222222-2222-4222-8222-222222222222",
  attempt_number: 1,
  status: "FAILED",
  leased_at: "2026-08-30T10:00:00Z",
  queue_entered_at: "2026-08-30T09:59:59Z",
  scheduled_at_snapshot: "2026-08-30T09:59:59Z",
  started_at: "2026-08-30T10:00:00.100Z",
  finished_at: "2026-08-30T10:00:00.200Z",
  retry_scheduled_at: "2026-08-30T10:00:01Z",
  recovered_lease_expires_at: null,
  recovered_at: null,
  recovery_action: null,
  output: null,
  error: "retryable failure",
  created_at: "2026-08-30T10:00:00Z",
  updated_at: "2026-08-30T10:00:00.200Z",
};

export const succeededAttempt: TaskAttempt = {
  ...failedAttempt,
  id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  worker_id: "33333333-3333-4333-8333-333333333333",
  attempt_number: 2,
  status: "SUCCEEDED",
  leased_at: "2026-08-30T10:00:01Z",
  started_at: "2026-08-30T10:00:01.100Z",
  finished_at: "2026-08-30T10:00:01.200Z",
  retry_scheduled_at: null,
  output: { echo: { message: "hello" } },
  error: null,
};

export const abandonedAttempt: TaskAttempt = {
  ...failedAttempt,
  status: "ABANDONED",
  error: "lease_expired",
  retry_scheduled_at: null,
  recovered_lease_expires_at: "2026-08-30T10:00:05Z",
  recovered_at: "2026-08-30T10:00:05.050Z",
  recovery_action: "requeued",
};

export const worker: Worker = {
  id: "22222222-2222-4222-8222-222222222222",
  instance_id: "worker-local-1",
  name: "worker-1",
  enabled: true,
  registered_at: "2026-08-30T09:00:00Z",
  last_heartbeat: "2026-08-30T10:00:01Z",
  updated_at: "2026-08-30T10:00:01Z",
  liveness: "ACTIVE",
  heartbeat_age_seconds: 1.2,
  metadata: { concurrency: 1 },
};

export const overview: Overview = {
  task_counts: {
    QUEUED: 2,
    LEASED: 0,
    RUNNING: 1,
    RETRYING: 1,
    SUCCEEDED: 42,
    FAILED: 3,
    CANCELLED: 0,
  },
  worker_counts: { ACTIVE: 1, STALE: 0, DEAD: 0 },
  recent_tasks: [task],
  recent_exceptions: [
    {
      task_id: task.id,
      task_type: task.task_type,
      attempt_number: 1,
      status: "FAILED",
      worker_id: failedAttempt.worker_id,
      error: failedAttempt.error,
      retry_scheduled_at: failedAttempt.retry_scheduled_at,
      recovered_at: null,
      recovery_action: null,
      occurred_at: failedAttempt.finished_at!,
    },
  ],
  observed_at: "2026-08-30T10:00:02Z",
};
