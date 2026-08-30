export type TaskStatus =
  | "QUEUED"
  | "LEASED"
  | "RUNNING"
  | "RETRYING"
  | "SUCCEEDED"
  | "FAILED"
  | "CANCELLED";

export type AttemptStatus =
  "LEASED" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELLED" | "ABANDONED";

export type WorkerLiveness = "ACTIVE" | "STALE" | "DEAD";

export interface Task {
  id: string;
  queue: string;
  task_type: string;
  payload: Record<string, unknown>;
  status: TaskStatus;
  priority: number;
  max_attempts: number;
  attempt_count: number;
  scheduled_at: string;
  queued_at: string;
  claimed_by_worker_id: string | null;
  lease_expires_at: string | null;
  completed_at: string | null;
  result: Record<string, unknown> | null;
  last_error: string | null;
  idempotency_key: string | null;
  created_at: string;
  updated_at: string;
}

export interface TaskAttempt {
  id: string;
  task_id: string;
  worker_id: string;
  attempt_number: number;
  status: AttemptStatus;
  leased_at: string;
  queue_entered_at: string | null;
  scheduled_at_snapshot: string | null;
  started_at: string | null;
  finished_at: string | null;
  retry_scheduled_at: string | null;
  recovered_lease_expires_at: string | null;
  recovered_at: string | null;
  recovery_action: string | null;
  output: Record<string, unknown> | null;
  error: string | null;
  created_at: string;
  updated_at: string;
}

export interface Worker {
  id: string;
  instance_id: string;
  name: string;
  enabled: boolean;
  registered_at: string;
  last_heartbeat: string | null;
  updated_at: string;
  liveness: WorkerLiveness;
  heartbeat_age_seconds: number | null;
  metadata: Record<string, unknown>;
}

export interface ExceptionalAttempt {
  task_id: string;
  task_type: string;
  attempt_number: number;
  status: AttemptStatus;
  worker_id: string;
  error: string | null;
  retry_scheduled_at: string | null;
  recovered_at: string | null;
  recovery_action: string | null;
  occurred_at: string;
}

export interface Overview {
  task_counts: Record<TaskStatus, number>;
  worker_counts: Record<WorkerLiveness, number>;
  recent_tasks: Task[];
  recent_exceptions: ExceptionalAttempt[];
  observed_at: string;
}

export interface TaskList {
  items: Task[];
  limit: number;
  offset: number;
  total: number;
}

export interface WorkerList {
  items: Worker[];
  limit: number;
  offset: number;
  total: number;
}

export interface TaskSubmission {
  task_type: string;
  payload: Record<string, unknown>;
  queue: string;
  priority: number;
  max_attempts: number;
  idempotency_key?: string;
}
