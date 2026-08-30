import type { ReactNode } from "react";

import { formatRelative, formatTimestamp, shortId } from "./format";
import { Link } from "./router";
import type { AttemptStatus, Task, TaskStatus, WorkerLiveness } from "./types";

export function StatusBadge({
  status,
}: {
  status: TaskStatus | AttemptStatus | WorkerLiveness;
}) {
  return (
    <span className={`status-badge status-${status.toLowerCase()}`}>
      {status}
    </span>
  );
}

export function EmptyState({
  title,
  children,
  action,
}: {
  title: string;
  children: ReactNode;
  action?: ReactNode;
}) {
  return (
    <div className="empty-state">
      <div className="empty-mark" aria-hidden="true">
        ∅
      </div>
      <h3>{title}</h3>
      <p>{children}</p>
      {action}
    </div>
  );
}

export function ErrorState({
  title = "TaskForge API unavailable",
  error,
  retry,
}: {
  title?: string;
  error: Error;
  retry: () => void;
}) {
  return (
    <div className="error-state" role="alert">
      <span className="status-dot danger" aria-hidden="true" />
      <div>
        <h3>{title}</h3>
        <p>
          {error.message}. The console is preserving unknown state rather than
          showing zeroes.
        </p>
      </div>
      <button className="button secondary" onClick={retry}>
        Try again
      </button>
    </div>
  );
}

export function LoadingCards({ count = 4 }: { count?: number }) {
  return (
    <div className="metric-grid" aria-label="Loading operational data">
      {Array.from({ length: count }, (_, index) => (
        <div className="skeleton metric-card" key={index} />
      ))}
    </div>
  );
}

export function RefreshControl({
  refreshing,
  lastUpdated,
  onRefresh,
}: {
  refreshing: boolean;
  lastUpdated: Date | null;
  onRefresh: () => void;
}) {
  return (
    <div className="refresh-control">
      <span>
        {lastUpdated
          ? `Updated ${lastUpdated.toLocaleTimeString()}`
          : "Not yet refreshed"}
      </span>
      <button
        className="icon-button"
        onClick={onRefresh}
        disabled={refreshing}
        aria-label="Refresh page data"
      >
        <span aria-hidden="true">↻</span>{" "}
        {refreshing ? "Refreshing" : "Refresh"}
      </button>
    </div>
  );
}

export function TaskTable({
  tasks,
  compact = false,
}: {
  tasks: Task[];
  compact?: boolean;
}) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Task</th>
            <th>Handler</th>
            <th>State</th>
            <th>Priority</th>
            <th>Attempts</th>
            <th>Created</th>
            {!compact && <th>Updated</th>}
          </tr>
        </thead>
        <tbody>
          {tasks.map((task) => (
            <tr key={task.id}>
              <td>
                <Link
                  to={`/tasks/${task.id}`}
                  className="mono-link"
                  title={task.id}
                >
                  {shortId(task.id)}
                </Link>
              </td>
              <td>
                <span className="handler-name">{task.task_type}</span>
                <small>{task.queue}</small>
              </td>
              <td>
                <StatusBadge status={task.status} />
              </td>
              <td>{task.priority}</td>
              <td>
                {task.attempt_count}/{task.max_attempts}
              </td>
              <td title={formatTimestamp(task.created_at)}>
                {formatRelative(task.created_at)}
              </td>
              {!compact && (
                <td title={formatTimestamp(task.updated_at)}>
                  {formatRelative(task.updated_at)}
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function JsonViewer({
  value,
  empty = "No data recorded",
}: {
  value: unknown;
  empty?: string;
}) {
  if (value === null || value === undefined)
    return <p className="muted">{empty}</p>;
  return (
    <pre className="json-viewer">
      <code>{JSON.stringify(value, null, 2)}</code>
    </pre>
  );
}

export function DefinitionList({
  items,
}: {
  items: Array<[string, ReactNode]>;
}) {
  return (
    <dl className="definition-list">
      {items.map(([label, value]) => (
        <div key={label}>
          <dt>{label}</dt>
          <dd>{value}</dd>
        </div>
      ))}
    </dl>
  );
}
