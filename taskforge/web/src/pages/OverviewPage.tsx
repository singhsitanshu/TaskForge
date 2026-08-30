import { useCallback } from "react";

import { getOverview, observabilityUrl } from "../api";
import {
  EmptyState,
  ErrorState,
  LoadingCards,
  RefreshControl,
  StatusBadge,
  TaskTable,
} from "../components";
import { formatRelative, formatTimestamp, shortId } from "../format";
import { usePollingResource } from "../hooks";
import { Link } from "../router";
import type { TaskStatus, WorkerLiveness } from "../types";

const TASK_STATES: TaskStatus[] = [
  "QUEUED",
  "LEASED",
  "RUNNING",
  "RETRYING",
  "SUCCEEDED",
  "FAILED",
  "CANCELLED",
];
const WORKER_STATES: WorkerLiveness[] = ["ACTIVE", "STALE", "DEAD"];

export function OverviewPage() {
  const loader = useCallback((signal: AbortSignal) => getOverview(signal), []);
  const state = usePollingResource(loader, 10_000);

  if (state.loading) return <LoadingCards count={7} />;
  if (!state.data && state.error)
    return <ErrorState error={state.error} retry={state.refresh} />;
  if (!state.data) return null;
  const overview = state.data;

  return (
    <div className="page-stack">
      <div className="page-intro">
        <div>
          <p className="eyebrow">Live control plane</p>
          <h1>Operations overview</h1>
          <p>
            Current durable task state, worker liveness, and exceptional
            execution activity.
          </p>
        </div>
        <RefreshControl {...state} onRefresh={state.refresh} />
      </div>
      {state.error && (
        <div className="inline-warning" role="status">
          Refresh failed; showing the last successful snapshot.
        </div>
      )}

      <section aria-labelledby="task-state-title">
        <div className="section-heading">
          <div>
            <h2 id="task-state-title">Task state</h2>
            <p>Exact PostgreSQL counts at the latest snapshot.</p>
          </div>
          <Link to="/tasks">Explore tasks →</Link>
        </div>
        <div className="metric-grid">
          {TASK_STATES.map((taskState) => (
            <article
              className={`metric-card metric-${taskState.toLowerCase()}`}
              key={taskState}
            >
              <span>{taskState}</span>
              <strong>
                {overview.task_counts[taskState].toLocaleString()}
              </strong>
              <small>durable tasks</small>
            </article>
          ))}
        </div>
      </section>

      <div className="overview-split">
        <section className="panel" aria-labelledby="workers-summary-title">
          <div className="section-heading">
            <div>
              <h2 id="workers-summary-title">Workers</h2>
              <p>Heartbeat classification from the API.</p>
            </div>
            <Link to="/workers">View workers →</Link>
          </div>
          <div className="worker-summary">
            {WORKER_STATES.map((workerState) => (
              <div key={workerState}>
                <StatusBadge status={workerState} />
                <strong>{overview.worker_counts[workerState]}</strong>
              </div>
            ))}
          </div>
        </section>
        <section className="panel" aria-labelledby="system-summary-title">
          <div className="section-heading">
            <div>
              <h2 id="system-summary-title">System</h2>
              <p>Only states the API can substantiate.</p>
            </div>
            <Link to="/system">Architecture →</Link>
          </div>
          <ul className="health-list">
            <li>
              <span>
                <i className="status-dot healthy" />
                API
              </span>
              <strong>Reachable</strong>
            </li>
            <li>
              <span>
                <i className="status-dot healthy" />
                PostgreSQL
              </span>
              <strong>Ready</strong>
            </li>
            <li>
              <span>
                <i className="status-dot unknown" />
                Scheduler
              </span>
              <strong>Not reported</strong>
            </li>
            <li>
              <span>
                <i className="status-dot unknown" />
                Redis
              </span>
              <strong>Not on execution path</strong>
            </li>
          </ul>
        </section>
      </div>

      <section className="panel" aria-labelledby="recent-tasks-title">
        <div className="section-heading">
          <div>
            <h2 id="recent-tasks-title">Recent tasks</h2>
            <p>Newest bounded task records from PostgreSQL.</p>
          </div>
          <Link className="button secondary small" to="/submit">
            + New task
          </Link>
        </div>
        {overview.recent_tasks.length ? (
          <TaskTable tasks={overview.recent_tasks} compact />
        ) : (
          <EmptyState
            title="No tasks yet"
            action={
              <Link className="button primary" to="/submit">
                Submit your first task
              </Link>
            }
          >
            Execution activity will appear here after a task is submitted.
          </EmptyState>
        )}
      </section>

      <section className="panel" aria-labelledby="exceptions-title">
        <div className="section-heading">
          <div>
            <h2 id="exceptions-title">Failure & recovery activity</h2>
            <p>Recent durable failed or abandoned attempts.</p>
          </div>
          <a
            href={observabilityUrl("grafana")}
            target="_blank"
            rel="noreferrer"
          >
            Open Grafana ↗
          </a>
        </div>
        {overview.recent_exceptions.length ? (
          <div className="activity-list">
            {overview.recent_exceptions.map((attempt) => (
              <Link
                to={`/tasks/${attempt.task_id}`}
                className="activity-row"
                key={`${attempt.task_id}-${attempt.attempt_number}`}
              >
                <StatusBadge status={attempt.status} />
                <span>
                  <strong>{attempt.task_type}</strong>
                  <small>
                    Attempt {attempt.attempt_number} · Task{" "}
                    {shortId(attempt.task_id)}
                  </small>
                </span>
                <span title={formatTimestamp(attempt.occurred_at)}>
                  {formatRelative(attempt.occurred_at)}
                </span>
              </Link>
            ))}
          </div>
        ) : (
          <EmptyState title="No exceptional activity">
            No failed or abandoned attempts were returned in this bounded
            window.
          </EmptyState>
        )}
      </section>
    </div>
  );
}
