import type {
  Overview,
  Task,
  TaskAttempt,
  TaskList,
  TaskStatus,
  TaskSubmission,
  WorkerList,
} from "./types";

const API_BASE = (import.meta.env.VITE_API_BASE_URL || "/api").replace(
  /\/$/,
  "",
);

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code?: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...init.headers },
  });
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    let code: string | undefined;
    try {
      const body = (await response.json()) as {
        detail?: string | { message?: string; code?: string };
      };
      if (typeof body.detail === "string") message = body.detail;
      if (body.detail && typeof body.detail === "object") {
        message = body.detail.message || message;
        code = body.detail.code;
      }
    } catch {
      // Keep the HTTP status when an upstream returns a non-JSON body.
    }
    throw new ApiError(message, response.status, code);
  }
  return (await response.json()) as T;
}

export function getOverview(signal?: AbortSignal): Promise<Overview> {
  return request<Overview>("/overview?recent_limit=8", { signal });
}

export function listTasks(
  options: {
    status?: TaskStatus;
    taskType?: string;
    queue?: string;
    limit: number;
    offset: number;
  },
  signal?: AbortSignal,
): Promise<TaskList> {
  const params = new URLSearchParams({
    limit: String(options.limit),
    offset: String(options.offset),
  });
  if (options.status) params.set("status", options.status);
  if (options.taskType) params.set("task_type", options.taskType);
  if (options.queue) params.set("queue", options.queue);
  return request<TaskList>(`/tasks?${params}`, { signal });
}

export function getTask(id: string, signal?: AbortSignal): Promise<Task> {
  return request<Task>(`/tasks/${encodeURIComponent(id)}`, { signal });
}

export async function getTaskDetail(
  id: string,
  signal?: AbortSignal,
): Promise<{ task: Task; attempts: TaskAttempt[] }> {
  const [task, attempts] = await Promise.all([
    getTask(id, signal),
    request<{ items: TaskAttempt[] }>(
      `/tasks/${encodeURIComponent(id)}/attempts`,
      { signal },
    ),
  ]);
  return { task, attempts: attempts.items };
}

export function listWorkers(
  limit: number,
  offset: number,
  signal?: AbortSignal,
): Promise<WorkerList> {
  return request<WorkerList>(`/workers?limit=${limit}&offset=${offset}`, {
    signal,
  });
}

export async function submitTask(
  submission: TaskSubmission,
  signal?: AbortSignal,
): Promise<{ task: Task; replayed: boolean }> {
  const { idempotency_key: idempotencyKey, ...body } = submission;
  const response = await fetch(`${API_BASE}/tasks`, {
    method: "POST",
    signal,
    headers: {
      "Content-Type": "application/json",
      ...(idempotencyKey ? { "Idempotency-Key": idempotencyKey } : {}),
    },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    const payload = (await response.json().catch(() => ({}))) as {
      detail?: string | { message?: string; code?: string };
    };
    const detail = payload.detail;
    throw new ApiError(
      typeof detail === "object"
        ? detail.message || "Task submission failed"
        : detail || "Task submission failed",
      response.status,
      typeof detail === "object" ? detail.code : undefined,
    );
  }
  return {
    task: (await response.json()) as Task,
    replayed: response.status === 200,
  };
}

export async function getReadiness(signal?: AbortSignal): Promise<boolean> {
  const response = await fetch(`${API_BASE}/readyz`, { signal });
  return response.ok;
}

export function observabilityUrl(kind: "grafana" | "prometheus"): string {
  const configured =
    kind === "grafana"
      ? import.meta.env.VITE_GRAFANA_URL
      : import.meta.env.VITE_PROMETHEUS_URL;
  if (configured) return configured;
  const port =
    kind === "grafana"
      ? import.meta.env.VITE_GRAFANA_PORT || "3001"
      : import.meta.env.VITE_PROMETHEUS_PORT || "9090";
  return `${window.location.protocol}//${window.location.hostname}:${port}`;
}
