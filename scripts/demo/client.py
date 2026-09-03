from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any


class DemoError(RuntimeError):
    """An expected, actionable demo failure."""


class ApiError(DemoError):
    def __init__(
        self,
        operation: str,
        endpoint: str,
        message: str,
        status: int | None = None,
        task_id: str | None = None,
    ) -> None:
        details = [f"operation={operation}", f"endpoint={endpoint}"]
        if status is not None:
            details.append(f"HTTP {status}")
        if task_id:
            details.append(f"task={task_id}")
        super().__init__(f"{message} ({', '.join(details)})")
        self.status = status


class APIClient:
    def __init__(self, base_url: str, request_timeout: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout

    def request(
        self,
        operation: str,
        path: str,
        *,
        method: str = "GET",
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        endpoint = f"{self.base_url}{path}"
        data = json.dumps(body).encode() if body is not None else None
        request_headers = {"Accept": "application/json", **(headers or {})}
        if data is not None:
            request_headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            endpoint, data=data, method=method, headers=request_headers
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self.request_timeout
            ) as response:
                payload = response.read()
        except urllib.error.HTTPError as error:
            message = _error_message(error.read())
            raise ApiError(operation, endpoint, message, error.code, task_id) from error
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            raise ApiError(
                operation,
                endpoint,
                str(error.reason if hasattr(error, "reason") else error),
            ) from error
        try:
            return json.loads(payload)
        except json.JSONDecodeError as error:
            raise ApiError(
                operation, endpoint, "server returned invalid JSON", task_id=task_id
            ) from error

    def ready(self) -> bool:
        try:
            return (
                self.request("check API readiness", "/readyz").get("status") == "ready"
            )
        except ApiError:
            return False

    def submit(
        self, submission: dict[str, Any], idempotency_key: str
    ) -> dict[str, Any]:
        return self.request(
            "submit task",
            "/tasks",
            method="POST",
            body=submission,
            headers={"Idempotency-Key": idempotency_key},
        )

    def task(self, task_id: str) -> dict[str, Any]:
        return self.request("get task", f"/tasks/{task_id}", task_id=task_id)

    def attempts(self, task_id: str) -> list[dict[str, Any]]:
        response = self.request(
            "list task attempts", f"/tasks/{task_id}/attempts", task_id=task_id
        )
        return response["items"]

    def workers(self) -> list[dict[str, Any]]:
        return self.request("list workers", "/workers?limit=100&offset=0")["items"]


def wait_for(
    fetch: Callable[[], Any],
    accept: Callable[[Any], bool],
    *,
    description: str,
    timeout: float,
    interval: float = 0.5,
    progress: Callable[[str], None] | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Any:
    started = clock()
    last_value: Any = None
    while True:
        last_value = fetch()
        if accept(last_value):
            return last_value
        elapsed = clock() - started
        if elapsed >= timeout:
            raise DemoError(
                f"Timed out after {elapsed:.1f}s waiting for {description}; "
                f"last value={_describe(last_value)}"
            )
        if progress:
            progress(f"Waiting for {description}... {elapsed:.1f}s")
        sleep(min(interval, max(0.0, timeout - elapsed)))


def wait_for_task_status(
    client: APIClient,
    task_id: str,
    statuses: set[str],
    *,
    timeout: float,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    return wait_for(
        lambda: client.task(task_id),
        lambda task: task.get("status") in statuses,
        description=f"task {task_id} to reach {', '.join(sorted(statuses))}",
        timeout=timeout,
        progress=progress,
    )


def _error_message(payload: bytes) -> str:
    try:
        detail = json.loads(payload).get("detail")
        if isinstance(detail, dict):
            return str(detail.get("message") or detail.get("code") or detail)
        if detail:
            return str(detail)
    except (json.JSONDecodeError, AttributeError):
        pass
    return payload.decode(errors="replace")[:300] or "request failed"


def _describe(value: Any) -> str:
    if isinstance(value, dict):
        fields = (
            "id",
            "status",
            "attempt_count",
            "claimed_by_worker_id",
            "lease_expires_at",
        )
        return str({field: value[field] for field in fields if field in value})
    if isinstance(value, list) and all(isinstance(item, dict) for item in value):
        return str(
            [
                {
                    "attempt_number": item.get("attempt_number"),
                    "status": item.get("status"),
                    "worker_id": item.get("worker_id"),
                }
                for item in value
            ]
        )
    return str(value)
