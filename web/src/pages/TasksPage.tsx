import { useCallback, useState, type FormEvent } from "react";

import { listTasks } from "../api";
import {
  EmptyState,
  ErrorState,
  LoadingCards,
  RefreshControl,
  TaskTable,
} from "../components";
import { usePollingResource } from "../hooks";
import { Link, navigate } from "../router";
import type { TaskStatus } from "../types";

const PAGE_SIZE = 20;
const TASK_STATES: TaskStatus[] = [
  "QUEUED",
  "LEASED",
  "RUNNING",
  "RETRYING",
  "SUCCEEDED",
  "FAILED",
  "CANCELLED",
];
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function TasksPage() {
  const [status, setStatus] = useState<TaskStatus | "">("");
  const [taskType, setTaskType] = useState("");
  const [queue, setQueue] = useState("");
  const [taskId, setTaskId] = useState("");
  const [searchError, setSearchError] = useState("");
  const [page, setPage] = useState(0);
  const loader = useCallback(
    (signal: AbortSignal) =>
      listTasks(
        {
          status: status || undefined,
          taskType: taskType.trim() || undefined,
          queue: queue.trim() || undefined,
          limit: PAGE_SIZE,
          offset: page * PAGE_SIZE,
        },
        signal,
      ),
    [page, queue, status, taskType],
  );
  const state = usePollingResource(loader, 15_000);

  const search = (event: FormEvent) => {
    event.preventDefault();
    const normalized = taskId.trim();
    if (!UUID_PATTERN.test(normalized)) {
      setSearchError("Enter a complete TaskForge UUID.");
      return;
    }
    navigate(`/tasks/${normalized}`);
  };
  const changeFilter = (setter: (value: string) => void, value: string) => {
    setter(value);
    setPage(0);
  };

  return (
    <div className="page-stack">
      <div className="page-intro">
        <div>
          <p className="eyebrow">Durable task state</p>
          <h1>Tasks</h1>
          <p>
            Search, filter, and inspect bounded task history without querying
            PostgreSQL directly.
          </p>
        </div>
        {state.data && <RefreshControl {...state} onRefresh={state.refresh} />}
      </div>
      <section className="filter-panel" aria-label="Task filters">
        <form className="id-search" onSubmit={search}>
          <label htmlFor="task-id">Open task by ID</label>
          <div>
            <input
              id="task-id"
              value={taskId}
              onChange={(event) => {
                setTaskId(event.target.value);
                setSearchError("");
              }}
              placeholder="Paste a full task UUID"
            />
            <button className="button secondary" type="submit">
              Open
            </button>
          </div>
          {searchError && <small className="field-error">{searchError}</small>}
        </form>
        <div className="filters">
          <label>
            Status
            <select
              value={status}
              onChange={(event) =>
                changeFilter(
                  (value) => setStatus(value as TaskStatus | ""),
                  event.target.value,
                )
              }
            >
              <option value="">All states</option>
              {TASK_STATES.map((item) => (
                <option key={item}>{item}</option>
              ))}
            </select>
          </label>
          <label>
            Handler
            <input
              value={taskType}
              onChange={(event) =>
                changeFilter(setTaskType, event.target.value)
              }
              placeholder="Exact handler"
            />
          </label>
          <label>
            Queue
            <input
              value={queue}
              onChange={(event) => changeFilter(setQueue, event.target.value)}
              placeholder="Exact queue"
            />
          </label>
        </div>
      </section>
      {state.loading && <LoadingCards count={3} />}
      {!state.data && state.error && (
        <ErrorState error={state.error} retry={state.refresh} />
      )}
      {state.data && (
        <section className="panel">
          <div className="section-heading">
            <div>
              <h2>Task records</h2>
              <p>
                {state.data.total.toLocaleString()} matching task
                {state.data.total === 1 ? "" : "s"}
              </p>
            </div>
            <Link className="button primary small" to="/submit">
              + New task
            </Link>
          </div>
          {state.error && (
            <div className="inline-warning">
              Refresh failed; showing saved rows.
            </div>
          )}
          {state.data.items.length ? (
            <TaskTable tasks={state.data.items} />
          ) : (
            <EmptyState title="No matching tasks">
              Change the filters or submit a new task.
            </EmptyState>
          )}
          <nav className="pagination" aria-label="Task pagination">
            <button
              className="button secondary"
              disabled={page === 0}
              onClick={() => setPage((value) => value - 1)}
            >
              ← Previous
            </button>
            <span>
              Page {page + 1} of{" "}
              {Math.max(1, Math.ceil(state.data.total / PAGE_SIZE))}
            </span>
            <button
              className="button secondary"
              disabled={(page + 1) * PAGE_SIZE >= state.data.total}
              onClick={() => setPage((value) => value + 1)}
            >
              Next →
            </button>
          </nav>
        </section>
      )}
    </div>
  );
}
