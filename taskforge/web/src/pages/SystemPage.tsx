import { useCallback } from "react";

import { getOverview, observabilityUrl } from "../api";
import { ErrorState, RefreshControl } from "../components";
import { usePollingResource } from "../hooks";

export function SystemPage() {
  const loader = useCallback((signal: AbortSignal) => getOverview(signal), []);
  const state = usePollingResource(loader, 15_000);
  return (
    <div className="page-stack">
      <div className="page-intro">
        <div>
          <p className="eyebrow">System model</p>
          <h1>Architecture & observability</h1>
          <p>
            PostgreSQL is the durable source of truth; Grafana owns historical
            metrics.
          </p>
        </div>
        {state.data && <RefreshControl {...state} onRefresh={state.refresh} />}
      </div>
      <section
        className="architecture-panel panel"
        aria-labelledby="architecture-title"
      >
        <div className="section-heading">
          <div>
            <h2 id="architecture-title">Execution architecture</h2>
            <p>
              Current service boundaries and authoritative communication paths.
            </p>
          </div>
        </div>
        <div
          className="architecture-flow"
          role="img"
          aria-label="Client requests flow through the TaskForge API to PostgreSQL. Workers claim and execute tasks from PostgreSQL. The scheduler promotes retries and recovers expired leases. API, worker, and scheduler metrics flow to Prometheus and Grafana."
        >
          <div className="arch-node">
            <span>Browser</span>
            <strong>Operations console</strong>
          </div>
          <span className="arch-arrow">→</span>
          <div className="arch-node accent">
            <span>Control plane</span>
            <strong>TaskForge API</strong>
          </div>
          <span className="arch-arrow">→</span>
          <div className="arch-node database">
            <span>Source of truth</span>
            <strong>PostgreSQL</strong>
          </div>
          <div className="arch-branches">
            <div className="arch-node">
              <span>Execution</span>
              <strong>Workers</strong>
            </div>
            <div className="arch-node">
              <span>Lifecycle maintenance</span>
              <strong>Scheduler</strong>
            </div>
          </div>
        </div>
        <p className="architecture-note">
          Redis is provisioned for future transient coordination and is not on
          the current execution or recovery path.
        </p>
      </section>
      <div className="capability-grid">
        {[
          [
            "Atomic claiming",
            "Priority-ordered FOR UPDATE SKIP LOCKED coordination.",
          ],
          [
            "Renewable leases",
            "Worker ownership expires safely when execution is lost.",
          ],
          [
            "Retries",
            "Typed retryable failures use delayed exponential backoff.",
          ],
          [
            "Crash recovery",
            "The scheduler abandons expired attempts and requeues eligible tasks.",
          ],
          [
            "Durable history",
            "Every attempt, worker, result, and error remains inspectable.",
          ],
          [
            "Idempotent submission",
            "A key prevents duplicate logical creation; execution remains at-least-once.",
          ],
        ].map(([title, copy]) => (
          <article className="capability-card" key={title}>
            <span aria-hidden="true">◇</span>
            <h3>{title}</h3>
            <p>{copy}</p>
          </article>
        ))}
      </div>
      <section aria-labelledby="service-health-title">
        <div className="section-heading">
          <div>
            <h2 id="service-health-title">Service evidence</h2>
            <p>
              Unknown components stay unknown rather than being presented as
              healthy.
            </p>
          </div>
        </div>
        {!state.data && state.error ? (
          <ErrorState
            title="System evidence unavailable"
            error={state.error}
            retry={state.refresh}
          />
        ) : (
          <div className="service-grid">
            <article className="service-card">
              <span
                className={`status-dot ${state.data ? "healthy" : "unknown"}`}
              />
              <div>
                <strong>API</strong>
                <p>{state.data ? "Reachable" : "Checking"}</p>
              </div>
            </article>
            <article className="service-card">
              <span
                className={`status-dot ${state.data ? "healthy" : "unknown"}`}
              />
              <div>
                <strong>PostgreSQL</strong>
                <p>
                  {state.data ? "Ready; overview query succeeded" : "Checking"}
                </p>
              </div>
            </article>
            <article className="service-card">
              <span className="status-dot unknown" />
              <div>
                <strong>Scheduler</strong>
                <p>Not reported through the public API</p>
              </div>
            </article>
            <article className="service-card">
              <span className="status-dot unknown" />
              <div>
                <strong>Redis</strong>
                <p>Not on the execution path</p>
              </div>
            </article>
          </div>
        )}
      </section>
      <section aria-labelledby="observability-title">
        <div className="section-heading">
          <div>
            <h2 id="observability-title">Observability</h2>
            <p>
              Use the purpose-built systems for historical and low-level
              metrics.
            </p>
          </div>
        </div>
        <div className="observability-grid">
          <a
            className="observability-card"
            href={observabilityUrl("grafana")}
            target="_blank"
            rel="noreferrer"
          >
            <span className="observability-kicker">Dashboards</span>
            <h3>Grafana</h3>
            <p>
              Explore throughput, latency, retry, recovery, worker, scheduler,
              and API metrics.
            </p>
            <strong>Open Grafana ↗</strong>
          </a>
          <a
            className="observability-card"
            href={observabilityUrl("prometheus")}
            target="_blank"
            rel="noreferrer"
          >
            <span className="observability-kicker">Raw metrics</span>
            <h3>Prometheus</h3>
            <p>Inspect TaskForge metrics directly and run PromQL queries.</p>
            <strong>Open Prometheus ↗</strong>
          </a>
        </div>
      </section>
    </div>
  );
}
