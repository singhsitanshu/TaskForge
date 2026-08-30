import { useCallback, useState } from "react";

import { listWorkers } from "../api";
import {
  EmptyState,
  ErrorState,
  LoadingCards,
  RefreshControl,
  StatusBadge,
} from "../components";
import { formatRelative, formatTimestamp, shortId } from "../format";
import { usePollingResource } from "../hooks";

const PAGE_SIZE = 25;

export function WorkersPage() {
  const [page, setPage] = useState(0);
  const loader = useCallback(
    (signal: AbortSignal) => listWorkers(PAGE_SIZE, page * PAGE_SIZE, signal),
    [page],
  );
  const state = usePollingResource(loader, 10_000);
  return (
    <div className="page-stack">
      <div className="page-intro">
        <div>
          <p className="eyebrow">Process liveness</p>
          <h1>Workers</h1>
          <p>
            Durable registration and API-derived heartbeat state. Liveness does
            not determine task ownership.
          </p>
        </div>
        {state.data && <RefreshControl {...state} onRefresh={state.refresh} />}
      </div>
      {state.loading && <LoadingCards count={3} />}
      {!state.data && state.error && (
        <ErrorState error={state.error} retry={state.refresh} />
      )}
      {state.data && (
        <section className="panel">
          <div className="section-heading">
            <div>
              <h2>Registered workers</h2>
              <p>
                {state.data.total.toLocaleString()} durable worker record
                {state.data.total === 1 ? "" : "s"}
              </p>
            </div>
          </div>
          {state.error && (
            <div className="inline-warning">
              Refresh failed; showing saved rows.
            </div>
          )}
          {state.data.items.length ? (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Worker</th>
                    <th>Liveness</th>
                    <th>Enabled</th>
                    <th>Last heartbeat</th>
                    <th>Registered</th>
                    <th>Metadata</th>
                  </tr>
                </thead>
                <tbody>
                  {state.data.items.map((worker) => (
                    <tr key={worker.id}>
                      <td>
                        <strong>{worker.name}</strong>
                        <small className="mono" title={worker.id}>
                          {shortId(worker.id)}
                        </small>
                        <small>{worker.instance_id}</small>
                      </td>
                      <td>
                        <StatusBadge status={worker.liveness} />
                        {worker.heartbeat_age_seconds !== null && (
                          <small>
                            {worker.heartbeat_age_seconds.toFixed(1)}s old
                          </small>
                        )}
                      </td>
                      <td>{worker.enabled ? "Enabled" : "Disabled"}</td>
                      <td title={formatTimestamp(worker.last_heartbeat)}>
                        {formatRelative(worker.last_heartbeat)}
                      </td>
                      <td title={formatTimestamp(worker.registered_at)}>
                        {formatRelative(worker.registered_at)}
                      </td>
                      <td>
                        <code>
                          {Object.keys(worker.metadata).length
                            ? JSON.stringify(worker.metadata)
                            : "{}"}
                        </code>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <EmptyState title="No workers detected">
              No durable worker registrations were returned. Start a worker and
              refresh this page.
            </EmptyState>
          )}
          <nav className="pagination" aria-label="Worker pagination">
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
