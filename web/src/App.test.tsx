import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import {
  abandonedAttempt,
  failedAttempt,
  overview,
  succeededAttempt,
  task,
  worker,
} from "./test/fixtures";

function response(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function installFetch(
  overrides: (url: string, init?: RequestInit) => Response | undefined = () =>
    undefined,
) {
  const mock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const overridden = overrides(url, init);
    if (overridden) return overridden;
    if (url.endsWith("/readyz")) return response({ status: "ready" });
    if (url.includes("/overview")) return response(overview);
    if (url.includes(`/tasks/${task.id}/attempts`))
      return response({ items: [failedAttempt, succeededAttempt] });
    if (url.endsWith(`/tasks/${task.id}`)) return response(task);
    if (url.includes("/tasks?"))
      return response({ items: [task], limit: 20, offset: 0, total: 1 });
    if (url.includes("/workers?"))
      return response({ items: [worker], limit: 25, offset: 0, total: 1 });
    return response({ detail: "not found" }, 404);
  });
  vi.stubGlobal("fetch", mock);
  return mock;
}

beforeEach(() => {
  window.history.replaceState({}, "", "/");
});

describe("operations overview", () => {
  it("renders real task and worker counts with navigation", async () => {
    installFetch();
    render(<App />);
    expect(
      await screen.findByRole("heading", { name: "Operations overview" }),
    ).toBeInTheDocument();
    expect(screen.getByText("42")).toBeInTheDocument();
    expect(screen.getAllByText("test.echo").length).toBeGreaterThan(0);
    expect(screen.getAllByText("ACTIVE").length).toBeGreaterThan(0);
    await userEvent.click(screen.getByRole("link", { name: /^Tasks$/ }));
    expect(
      await screen.findByRole("heading", { name: "Tasks" }),
    ).toBeInTheDocument();
  });

  it("shows a deliberate loading state", () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() => new Promise<Response>(() => undefined)),
    );
    render(<App />);
    expect(
      screen.getByLabelText("Loading operational data"),
    ).toBeInTheDocument();
  });

  it("shows an API failure instead of zero values", async () => {
    installFetch((url) =>
      url.includes("/overview")
        ? response({ detail: "database unavailable" }, 503)
        : undefined,
    );
    render(<App />);
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "database unavailable",
    );
    expect(screen.queryByText("durable tasks")).not.toBeInTheDocument();
  });

  it("renders the empty database state", async () => {
    installFetch((url) =>
      url.includes("/overview")
        ? response({
            ...overview,
            recent_tasks: [],
            recent_exceptions: [],
            task_counts: {
              QUEUED: 0,
              LEASED: 0,
              RUNNING: 0,
              RETRYING: 0,
              SUCCEEDED: 0,
              FAILED: 0,
              CANCELLED: 0,
            },
          })
        : undefined,
    );
    render(<App />);
    expect(await screen.findByText("No tasks yet")).toBeInTheDocument();
    expect(screen.getByText("No exceptional activity")).toBeInTheDocument();
  });
});

describe("task explorer", () => {
  it("renders bounded records and applies server-side filters", async () => {
    window.history.replaceState({}, "", "/tasks");
    const fetchMock = installFetch();
    render(<App />);
    expect(await screen.findByText("1 matching task")).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByLabelText("Status"), "SUCCEEDED");
    await waitFor(() =>
      expect(
        fetchMock.mock.calls.some(([url]) =>
          String(url).includes("status=SUCCEEDED"),
        ),
      ).toBe(true),
    );
    expect(screen.getByRole("button", { name: "Next →" })).toBeDisabled();
  });

  it("validates exact task-ID navigation", async () => {
    window.history.replaceState({}, "", "/tasks");
    installFetch();
    render(<App />);
    await screen.findByText("1 matching task");
    await userEvent.type(
      screen.getByLabelText("Open task by ID"),
      "not-a-uuid",
    );
    await userEvent.click(screen.getByRole("button", { name: "Open" }));
    expect(
      screen.getByText("Enter a complete TaskForge UUID."),
    ).toBeInTheDocument();
  });
});

describe("task detail and attempt history", () => {
  it("renders payload, result, retry history, and distinct attempt timing", async () => {
    window.history.replaceState({}, "", `/tasks/${task.id}`);
    installFetch();
    render(<App />);
    expect(
      await screen.findByRole("heading", { name: "Durable attempt history" }),
    ).toBeInTheDocument();
    expect(
      screen.getByText("Retry scheduled for Aug 30, 2026, 5:00:01 AM"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("retryable failure", { selector: "code" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { name: "Final result" }),
    ).toBeInTheDocument();
    expect(screen.getAllByText("Attempt lifecycle")).toHaveLength(2);
  });

  it("represents abandonment and replacement without calling it ordinary failure", async () => {
    window.history.replaceState({}, "", `/tasks/${task.id}`);
    installFetch((url) =>
      url.includes("/attempts")
        ? response({ items: [abandonedAttempt, succeededAttempt] })
        : undefined,
    );
    render(<App />);
    expect(
      await screen.findByText("Recovered after lease expiration · requeued"),
    ).toBeInTheDocument();
    expect(screen.getByText("ABANDONED")).toBeInTheDocument();
    expect(screen.getByText("Lease expired")).toBeInTheDocument();
  });

  it("handles a missing task", async () => {
    window.history.replaceState(
      {},
      "",
      "/tasks/99999999-9999-4999-8999-999999999999",
    );
    installFetch((url) =>
      url.includes("99999999")
        ? response({ detail: "task not found" }, 404)
        : undefined,
    );
    render(<App />);
    expect(await screen.findByText("Task not found")).toBeInTheDocument();
  });
});

describe("task submission", () => {
  async function fillValidForm() {
    await userEvent.type(
      screen.getByLabelText(/Handler \/ task type/),
      "test.echo",
    );
    const payload = screen.getByLabelText(/JSON payload/);
    fireEvent.change(payload, {
      target: { value: JSON.stringify({ message: "hello" }) },
    });
  }

  it("rejects invalid JSON before contacting the API", async () => {
    window.history.replaceState({}, "", "/submit");
    const fetchMock = installFetch();
    render(<App />);
    await userEvent.type(
      screen.getByLabelText(/Handler \/ task type/),
      "test.echo",
    );
    const payload = screen.getByLabelText(/JSON payload/);
    fireEvent.change(payload, { target: { value: "{" } });
    await userEvent.click(screen.getByRole("button", { name: "Submit task" }));
    expect(screen.getByRole("alert")).toHaveTextContent("valid JSON");
    expect(
      fetchMock.mock.calls.filter(([url]) => String(url).endsWith("/tasks")),
    ).toHaveLength(0);
  });

  it("surfaces successful creation and idempotent replay distinctly", async () => {
    window.history.replaceState({}, "", "/submit");
    installFetch((url, init) =>
      url.endsWith("/tasks") && init?.method === "POST"
        ? response(task, 200)
        : undefined,
    );
    render(<App />);
    await fillValidForm();
    await userEvent.type(
      screen.getByLabelText(/Idempotency key/),
      "console-demo",
    );
    await userEvent.click(screen.getByRole("button", { name: "Submit task" }));
    expect(
      await screen.findByText("Existing task returned"),
    ).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "View task →" })).toHaveAttribute(
      "href",
      `/tasks/${task.id}`,
    );
  });

  it("explains idempotency conflicts", async () => {
    window.history.replaceState({}, "", "/submit");
    installFetch((url, init) =>
      url.endsWith("/tasks") && init?.method === "POST"
        ? response(
            { detail: { code: "IDEMPOTENCY_KEY_REUSE", message: "conflict" } },
            409,
          )
        : undefined,
    );
    render(<App />);
    await fillValidForm();
    await userEvent.click(screen.getByRole("button", { name: "Submit task" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "already belongs to a different semantic submission",
    );
  });
});

describe("workers and system", () => {
  it("renders authoritative worker liveness and heartbeat evidence", async () => {
    window.history.replaceState({}, "", "/workers");
    installFetch();
    render(<App />);
    expect(await screen.findByText("worker-1")).toBeInTheDocument();
    expect(screen.getByText("ACTIVE")).toBeInTheDocument();
    expect(screen.getByText("1.2s old")).toBeInTheDocument();
  });

  it("does not claim scheduler or Redis health", async () => {
    window.history.replaceState({}, "", "/system");
    installFetch();
    render(<App />);
    expect(
      await screen.findByText("Not reported through the public API"),
    ).toBeInTheDocument();
    expect(
      screen.getAllByText("Not on the execution path").length,
    ).toBeGreaterThan(0);
    expect(screen.getByRole("link", { name: /Open Grafana/ })).toHaveAttribute(
      "href",
      "http://localhost:3001",
    );
  });
});
