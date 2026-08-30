import { useCallback, useState } from "react";

import { ApiError, getTaskDetail } from "../api";
import {
  DefinitionList,
  EmptyState,
  ErrorState,
  JsonViewer,
  RefreshControl,
  StatusBadge,
} from "../components";
import { formatDuration, formatTimestamp, shortId } from "../format";
import { usePollingResource } from "../hooks";
import { Link } from "../router";
import type { TaskAttempt } from "../types";

const TERMINAL = new Set(["SUCCEEDED", "FAILED", "CANCELLED"]);

function AttemptConnector({ attempt }: { attempt: TaskAttempt }) {
  if (attempt.status === "ABANDONED")
    return (
      <div className="attempt-connector">
        <span>↓</span> Recovered after lease expiration
        {attempt.recovery_action ? ` · ${attempt.recovery_action}` : ""}
      </div>
    );
  if (attempt.status === "FAILED" && attempt.retry_scheduled_at)
    return (
      <div className="attempt-connector">
        <span>↓</span> Retry scheduled for{" "}
        {formatTimestamp(attempt.retry_scheduled_at)}
      </div>
    );
  return (
    <div className="attempt-connector">
      <span>↓</span> Next durable attempt
    </div>
  );
}

function AttemptCard({ attempt }: { attempt: TaskAttempt }) {
  return (
    <article className={`attempt-card attempt-${attempt.status.toLowerCase()}`}>
      <div className="attempt-heading">
        <div>
          <span>Attempt {attempt.attempt_number}</span>
          <strong>{shortId(attempt.id)}</strong>
        </div>
        <StatusBadge status={attempt.status} />
      </div>
      <DefinitionList
        items={[
          [
            "Worker",
            <span className="mono" title={attempt.worker_id}>
              {shortId(attempt.worker_id)}
            </span>,
          ],
          ["Queued", formatTimestamp(attempt.queue_entered_at)],
          ["Leased", formatTimestamp(attempt.leased_at)],
          ["Started", formatTimestamp(attempt.started_at)],
          ["Finished", formatTimestamp(attempt.finished_at)],
          [
            "Attempt lifecycle",
            formatDuration(attempt.leased_at, attempt.finished_at),
          ],
          ...(attempt.recovered_lease_expires_at
            ? [
                [
                  "Lease expired",
                  formatTimestamp(attempt.recovered_lease_expires_at),
                ] as [string, string],
              ]
            : []),
          ...(attempt.recovered_at
            ? [
                ["Recovered", formatTimestamp(attempt.recovered_at)] as [
                  string,
                  string,
                ],
              ]
            : []),
        ]}
      />
      {attempt.error && (
        <div className="attempt-error">
          <span>Error</span>
          <code>{attempt.error}</code>
        </div>
      )}
      {attempt.output && (
        <div>
          <h4>Attempt output</h4>
          <JsonViewer value={attempt.output} />
        </div>
      )}
    </article>
  );
}

export function TaskDetailPage({ id }: { id: string }) {
  const [terminal, setTerminal] = useState(false);
  const loader = useCallback(
    async (signal: AbortSignal) => {
      const detail = await getTaskDetail(id, signal);
      setTerminal(TERMINAL.has(detail.task.status));
      return detail;
    },
    [id],
  );
  const state = usePollingResource(loader, terminal ? null : 5_000);

  if (state.loading)
    return (
      <div
        className="detail-skeleton skeleton"
        aria-label="Loading task detail"
      />
    );
  if (!state.data && state.error) {
    if (state.error instanceof ApiError && state.error.status === 404)
      return (
        <EmptyState
          title="Task not found"
          action={
            <Link className="button secondary" to="/tasks">
              Back to tasks
            </Link>
          }
        >
          No persisted task exists for this identifier.
        </EmptyState>
      );
    return <ErrorState error={state.error} retry={state.refresh} />;
  }
  if (!state.data) return null;
  const { task, attempts } = state.data;

  return (
    <div className="page-stack">
      <div className="page-intro detail-intro">
        <div>
          <Link className="back-link" to="/tasks">
            ← Tasks
          </Link>
          <p className="eyebrow">Task lifecycle</p>
          <h1>{task.task_type}</h1>
          <div className="title-status">
            <StatusBadge status={task.status} />
            <code title={task.id}>{task.id}</code>
            <button
              className="copy-button"
              onClick={() => void navigator.clipboard.writeText(task.id)}
            >
              Copy ID
            </button>
          </div>
        </div>
        <RefreshControl {...state} onRefresh={state.refresh} />
      </div>
      {state.error && (
        <div className="inline-warning">
          Refresh failed; showing the last successful task state.
        </div>
      )}
      <div className="detail-grid">
        <section className="panel">
          <h2>Task summary</h2>
          <DefinitionList
            items={[
              ["Status", <StatusBadge status={task.status} />],
              ["Queue", task.queue],
              ["Priority", task.priority],
              ["Attempts", `${task.attempt_count} of ${task.max_attempts}`],
              ["Created", formatTimestamp(task.created_at)],
              ["Queued", formatTimestamp(task.queued_at)],
              ["Scheduled", formatTimestamp(task.scheduled_at)],
              ["Completed", formatTimestamp(task.completed_at)],
              [
                "Current worker",
                task.claimed_by_worker_id ? (
                  <span className="mono">
                    {shortId(task.claimed_by_worker_id)}
                  </span>
                ) : (
                  "No active owner"
                ),
              ],
              ["Lease expires", formatTimestamp(task.lease_expires_at)],
              ["Idempotency key", task.idempotency_key || "Not provided"],
            ]}
          />
        </section>
        <section className="panel">
          <h2>Payload</h2>
          <p className="section-note">
            Persisted JSON; rendered as inert text.
          </p>
          <JsonViewer value={task.payload} />
        </section>
      </div>
      {(task.result || task.last_error) && (
        <div className="detail-grid">
          {task.result && (
            <section className="panel">
              <h2>Final result</h2>
              <JsonViewer value={task.result} />
            </section>
          )}
          {task.last_error && (
            <section className="panel error-panel">
              <h2>Latest task error</h2>
              <pre className="error-copy">{task.last_error}</pre>
            </section>
          )}
        </div>
      )}
      <section aria-labelledby="attempts-title">
        <div className="section-heading">
          <div>
            <h2 id="attempts-title">Durable attempt history</h2>
            <p>
              At-least-once execution may create multiple attempts. External
              side effects still require application-level idempotency.
            </p>
          </div>
          <span className="count-pill">{attempts.length} recorded</span>
        </div>
        {attempts.length ? (
          <div className="attempt-timeline">
            {attempts.map((attempt, index) => (
              <div key={attempt.id}>
                <AttemptCard attempt={attempt} />
                {index < attempts.length - 1 && (
                  <AttemptConnector attempt={attempt} />
                )}
              </div>
            ))}
          </div>
        ) : (
          <EmptyState title="No attempts yet">
            The task has not been claimed by a worker.
          </EmptyState>
        )}
      </section>
    </div>
  );
}
