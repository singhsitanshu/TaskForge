from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .client import DemoError

TERMINAL_TASK_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}


def attempt_history(attempts: list[dict[str, Any]]) -> list[str]:
    return [str(attempt.get("status")) for attempt in attempts]


def attempt_duration(attempt: dict[str, Any]) -> str:
    started = attempt.get("started_at") or attempt.get("leased_at")
    finished = attempt.get("finished_at")
    if not started or not finished:
        return "not reported"
    started_at = datetime.fromisoformat(str(started).replace("Z", "+00:00"))
    finished_at = datetime.fromisoformat(str(finished).replace("Z", "+00:00"))
    return f"{(finished_at - started_at).total_seconds():.2f}s"


def validate_normal(task: dict[str, Any], attempts: list[dict[str, Any]]) -> None:
    observed = attempt_history(attempts)
    if task.get("status") != "SUCCEEDED" or observed != ["SUCCEEDED"]:
        raise DemoError(
            "Normal demo failed: expected SUCCEEDED in one attempt; "
            f"task={task.get('status')} attempts={' -> '.join(observed) or 'none'}"
        )


def validate_retry(task: dict[str, Any], attempts: list[dict[str, Any]]) -> None:
    observed = attempt_history(attempts)
    expected = ["FAILED", "SUCCEEDED"]
    if task.get("status") != "SUCCEEDED" or observed != expected:
        raise DemoError(
            "Retry demo failed: expected FAILED -> SUCCEEDED; "
            f"task={task.get('status')} attempts={' -> '.join(observed) or 'none'}"
        )


def validate_recovery(
    task: dict[str, Any], attempts: list[dict[str, Any]], captured_attempt_id: str
) -> None:
    captured = next(
        (attempt for attempt in attempts if attempt.get("id") == captured_attempt_id),
        None,
    )
    if captured is None or captured.get("status") != "ABANDONED":
        raise DemoError(
            "Recovery demo failed: captured owner attempt was not ABANDONED"
        )
    captured_number = int(captured["attempt_number"])
    later_success = any(
        int(attempt["attempt_number"]) > captured_number
        and attempt.get("status") == "SUCCEEDED"
        for attempt in attempts
    )
    if task.get("status") != "SUCCEEDED" or not later_success:
        raise DemoError(
            "Recovery demo failed: expected a later SUCCEEDED replacement; "
            f"task={task.get('status')} attempts={' -> '.join(attempt_history(attempts))}"
        )


def demo_dataset() -> list[dict[str, Any]]:
    submissions: list[dict[str, Any]] = []
    for index, priority in enumerate((0, 10, 25, 50, 100, 10, 25, 50, 0, 100), 1):
        submissions.append(
            {
                "label": f"success-{index:02d}",
                "task_type": "test.noop",
                "payload": {},
                "priority": priority,
                "max_attempts": 1,
                "expected": "SUCCEEDED",
            }
        )
    for index, duration in enumerate((150, 400, 800), 1):
        submissions.append(
            {
                "label": f"sleep-{index:02d}",
                "task_type": "test.sleep",
                "payload": {"duration_ms": duration},
                "priority": (10, 25, 50)[index - 1],
                "max_attempts": 1,
                "expected": "SUCCEEDED",
            }
        )
    for index in range(1, 3):
        submissions.append(
            {
                "label": f"retry-{index:02d}",
                "task_type": "test.fail_n_then_succeed",
                "payload": {"failures": 1},
                "priority": 25,
                "max_attempts": 2,
                "expected": "SUCCEEDED",
            }
        )
    submissions.append(
        {
            "label": "terminal-failure-01",
            "task_type": "test.fail_terminal",
            "payload": {},
            "priority": 0,
            "max_attempts": 1,
            "expected": "FAILED",
        }
    )
    return submissions


@dataclass(frozen=True)
class Container:
    container_id: str
    hostname: str
    name: str
    restart_policy: str


def select_owner_container(
    worker: dict[str, Any], containers: list[Container]
) -> Container:
    worker_name = str(worker.get("name", ""))
    instance_id = str(worker.get("instance_id", ""))
    matches = [
        container
        for container in containers
        if worker_name == container.hostname
        or instance_id == container.hostname
        or instance_id.startswith(f"{container.hostname}-")
    ]
    if len(matches) != 1:
        summary = (
            ", ".join(
                f"{container.name}({container.hostname})" for container in containers
            )
            or "none"
        )
        raise DemoError(
            "Cannot safely map task owner to exactly one local worker container. "
            f"worker name={worker_name!r} instance_id={instance_id!r}; containers={summary}"
        )
    return matches[0]
