import { useCallback, useEffect, useState, type ReactNode } from "react";

import { getReadiness, observabilityUrl } from "./api";
import { OverviewPage } from "./pages/OverviewPage";
import { SubmitPage } from "./pages/SubmitPage";
import { SystemPage } from "./pages/SystemPage";
import { TaskDetailPage } from "./pages/TaskDetailPage";
import { TasksPage } from "./pages/TasksPage";
import { WorkersPage } from "./pages/WorkersPage";
import { Link, usePathname } from "./router";

const NAVIGATION = [
  ["/", "Overview"],
  ["/tasks", "Tasks"],
  ["/workers", "Workers"],
  ["/system", "System"],
] as const;

function AppShell({
  pathname,
  children,
}: {
  pathname: string;
  children: ReactNode;
}) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [apiState, setApiState] = useState<
    "checking" | "healthy" | "unavailable"
  >("checking");
  const checkApi = useCallback(async () => {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 3_000);
    try {
      setApiState(
        (await getReadiness(controller.signal)) ? "healthy" : "unavailable",
      );
    } catch {
      setApiState("unavailable");
    } finally {
      window.clearTimeout(timeout);
    }
  }, []);
  useEffect(() => {
    void checkApi();
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void checkApi();
    }, 15_000);
    return () => window.clearInterval(timer);
  }, [checkApi]);
  useEffect(() => setMenuOpen(false), [pathname]);

  const isActive = (href: string) =>
    href === "/"
      ? pathname === "/"
      : pathname === href || pathname.startsWith(`${href}/`);
  return (
    <div className="app-shell">
      <a className="skip-link" href="#main-content">
        Skip to content
      </a>
      <aside
        className={menuOpen ? "sidebar open" : "sidebar"}
        aria-label="Primary navigation"
      >
        <div className="brand">
          <span className="brand-mark">TF</span>
          <div>
            <strong>TaskForge</strong>
            <small>Operations Console</small>
          </div>
        </div>
        <nav>
          <span className="nav-label">Workspace</span>
          {NAVIGATION.map(([href, label]) => (
            <Link
              key={href}
              to={href}
              className={isActive(href) ? "nav-link active" : "nav-link"}
            >
              <span className="nav-glyph" aria-hidden="true">
                {label.slice(0, 1)}
              </span>
              {label}
            </Link>
          ))}
          <Link
            to="/submit"
            className={
              isActive("/submit")
                ? "nav-link new-task active"
                : "nav-link new-task"
            }
          >
            <span aria-hidden="true">＋</span> New task
          </Link>
          <span className="nav-label observability-label">Observability</span>
          <a
            className="nav-link"
            href={observabilityUrl("grafana")}
            target="_blank"
            rel="noreferrer"
          >
            <span className="nav-glyph" aria-hidden="true">
              G
            </span>
            Grafana <small>↗</small>
          </a>
          <a
            className="nav-link"
            href={observabilityUrl("prometheus")}
            target="_blank"
            rel="noreferrer"
          >
            <span className="nav-glyph" aria-hidden="true">
              P
            </span>
            Prometheus <small>↗</small>
          </a>
        </nav>
        <div className="sidebar-foot">
          <span
            className={`status-dot ${apiState === "healthy" ? "healthy" : apiState === "unavailable" ? "danger" : "unknown"}`}
          />
          <div>
            <strong>
              {apiState === "healthy"
                ? "API connected"
                : apiState === "unavailable"
                  ? "API unavailable"
                  : "Checking API"}
            </strong>
            <small>PostgreSQL control plane</small>
          </div>
        </div>
      </aside>
      {menuOpen && (
        <button
          className="sidebar-scrim"
          aria-label="Close navigation"
          onClick={() => setMenuOpen(false)}
        />
      )}
      <div className="app-main">
        <header className="topbar">
          <button
            className="menu-button"
            aria-label="Open navigation"
            aria-expanded={menuOpen}
            onClick={() => setMenuOpen((open) => !open)}
          >
            ☰
          </button>
          <div className="topbar-status">
            <span
              className={`status-dot ${apiState === "healthy" ? "healthy" : apiState === "unavailable" ? "danger" : "unknown"}`}
            />
            <span>
              {apiState === "healthy"
                ? "System reachable"
                : apiState === "unavailable"
                  ? "Disconnected"
                  : "Connecting"}
            </span>
          </div>
          <Link className="button primary small" to="/submit">
            + New task
          </Link>
        </header>
        <main id="main-content" className="content">
          {children}
        </main>
      </div>
    </div>
  );
}

function route(pathname: string): ReactNode {
  if (pathname === "/" || pathname === "/overview") return <OverviewPage />;
  if (pathname === "/tasks") return <TasksPage />;
  if (pathname === "/submit") return <SubmitPage />;
  if (pathname === "/workers") return <WorkersPage />;
  if (pathname === "/system") return <SystemPage />;
  const taskMatch = pathname.match(/^\/tasks\/([^/]+)$/);
  if (taskMatch)
    return <TaskDetailPage id={decodeURIComponent(taskMatch[1])} />;
  return (
    <div className="empty-state">
      <div className="empty-mark">404</div>
      <h1>Page not found</h1>
      <p>The requested console route does not exist.</p>
      <Link className="button secondary" to="/">
        Return to overview
      </Link>
    </div>
  );
}

export function App() {
  const pathname = usePathname();
  return <AppShell pathname={pathname}>{route(pathname)}</AppShell>;
}
