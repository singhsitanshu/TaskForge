import { useState, type FormEvent } from "react";

import { ApiError, submitTask } from "../api";
import { StatusBadge } from "../components";
import { shortId } from "../format";
import { Link } from "../router";
import type { Task } from "../types";

export function SubmitPage() {
  const [taskType, setTaskType] = useState("");
  const [payload, setPayload] = useState("{\n  \n}");
  const [queue, setQueue] = useState("default");
  const [priority, setPriority] = useState(0);
  const [maxAttempts, setMaxAttempts] = useState(3);
  const [idempotencyKey, setIdempotencyKey] = useState("");
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState<{
    task: Task;
    replayed: boolean;
  } | null>(null);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setError("");
    setResult(null);
    let parsed: unknown;
    try {
      parsed = JSON.parse(payload);
    } catch {
      setError("Payload must be valid JSON.");
      return;
    }
    if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
      setError("Payload must be a JSON object.");
      return;
    }
    setSubmitting(true);
    try {
      setResult(
        await submitTask({
          task_type: taskType,
          payload: parsed as Record<string, unknown>,
          queue,
          priority,
          max_attempts: maxAttempts,
          ...(idempotencyKey.trim()
            ? { idempotency_key: idempotencyKey.trim() }
            : {}),
        }),
      );
    } catch (caught) {
      if (caught instanceof ApiError && caught.code === "IDEMPOTENCY_KEY_REUSE")
        setError(
          "That idempotency key already belongs to a different semantic submission. Reuse the original request or choose a new key.",
        );
      else
        setError(
          caught instanceof Error ? caught.message : "Task submission failed.",
        );
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="page-stack narrow-page">
      <div className="page-intro">
        <div>
          <p className="eyebrow">Control plane</p>
          <h1>New task</h1>
          <p>Submit durable work through the real TaskForge API.</p>
        </div>
      </div>
      <form
        className="submission-form panel"
        onSubmit={(event) => void submit(event)}
      >
        <div className="form-grid">
          <label className="span-two">
            Handler / task type
            <input
              required
              maxLength={255}
              value={taskType}
              onChange={(event) => setTaskType(event.target.value)}
              placeholder="test.echo"
            />
            <small>
              Free text preserves the API allowlist as the authority.
            </small>
          </label>
          <label>
            Queue
            <input
              required
              maxLength={128}
              value={queue}
              onChange={(event) => setQueue(event.target.value)}
            />
          </label>
          <label>
            Priority
            <input
              type="number"
              min={-32768}
              max={32767}
              value={priority}
              onChange={(event) => setPriority(Number(event.target.value))}
            />
          </label>
          <label>
            Maximum attempts
            <input
              type="number"
              min={1}
              max={100}
              value={maxAttempts}
              onChange={(event) => setMaxAttempts(Number(event.target.value))}
            />
          </label>
          <label>
            Idempotency key <span>(optional)</span>
            <input
              maxLength={255}
              pattern="[A-Za-z0-9._~:/+=-]+"
              value={idempotencyKey}
              onChange={(event) => setIdempotencyKey(event.target.value)}
            />
          </label>
          <label className="span-two">
            JSON payload
            <textarea
              rows={12}
              value={payload}
              onChange={(event) => setPayload(event.target.value)}
              spellCheck={false}
            />
            <small>
              Validated locally as an object; backend validation remains
              authoritative.
            </small>
          </label>
        </div>
        {error && (
          <div className="form-error" role="alert">
            {error}
          </div>
        )}
        <div className="form-actions">
          <span>
            Submission idempotency does not imply exactly-once execution.
          </span>
          <button className="button primary" disabled={submitting}>
            {submitting ? "Submitting…" : "Submit task"}
          </button>
        </div>
      </form>
      {result && (
        <section className="success-state" aria-live="polite">
          <div className="success-icon" aria-hidden="true">
            ✓
          </div>
          <div>
            <p className="eyebrow">
              {result.replayed ? "Idempotent replay" : "Task created"}
            </p>
            <h2>
              {result.replayed
                ? "Existing task returned"
                : "Submission accepted"}
            </h2>
            <p>
              <code title={result.task.id}>{shortId(result.task.id)}</code>{" "}
              <StatusBadge status={result.task.status} />
            </p>
            <p>
              {result.replayed
                ? "The server matched this request to the existing logical task."
                : "The durable task is ready for worker processing."}
            </p>
          </div>
          <Link className="button primary" to={`/tasks/${result.task.id}`}>
            View task →
          </Link>
        </section>
      )}
    </div>
  );
}
