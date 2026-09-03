from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from typing import Any

from .client import APIClient, DemoError, wait_for, wait_for_task_status
from .config import DemoConfig, resolve_config
from .runtime import (
    assert_demo_preconditions,
    collect_status,
    container_running,
    kill_and_restore,
    owner_container,
    require_safe_local_docker,
)
from .scenarios import (
    TERMINAL_TASK_STATES,
    attempt_duration,
    attempt_history,
    demo_dataset,
    validate_normal,
    validate_recovery,
    validate_retry,
)


def progress(message: str) -> None:
    print(f"  {message}", flush=True)


def run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:8]


def run_demo(
    config: DemoConfig, client: APIClient, *, json_output: bool = False
) -> dict[str, Any]:
    if not json_output:
        print("TaskForge Demo\n" + "─" * 40)
        print("[1/4] Checking TaskForge services", end=" ", flush=True)
    assert_demo_preconditions(config, client)
    if not json_output:
        print("PASS")

    identifier = run_id()
    if not json_output:
        print("[2/4] Submitting successful task", flush=True)
    normal = client.submit(
        {
            "task_type": "test.sleep",
            "payload": {"duration_ms": 500},
            "priority": 50,
            "max_attempts": 1,
        },
        f"tf014-demo-{identifier}-normal",
    )
    normal = wait_for_task_status(
        client,
        normal["id"],
        TERMINAL_TASK_STATES,
        timeout=30,
        progress=None if json_output else progress,
    )
    normal_attempts = client.attempts(normal["id"])
    validate_normal(normal, normal_attempts)
    if not json_output:
        print("      Successful task verified           PASS")

    if not json_output:
        print("[3/4] Running retry demonstration", flush=True)
    retry = client.submit(
        {
            "task_type": "test.fail_n_then_succeed",
            "payload": {"failures": 1},
            "priority": 50,
            "max_attempts": 2,
        },
        f"tf014-demo-{identifier}-retry",
    )
    retry = wait_for_task_status(
        client,
        retry["id"],
        TERMINAL_TASK_STATES,
        timeout=45,
        progress=None if json_output else progress,
    )
    retry_attempts = client.attempts(retry["id"])
    validate_retry(retry, retry_attempts)
    if not json_output:
        print("      Retry task verified                PASS")
        print("[4/4] Verifying durable attempt history  PASS")

    result = {
        "result": "PASS",
        "run_id": identifier,
        "tasks": {
            "normal": normal["id"],
            "retry": retry["id"],
        },
        "normal": {
            "state": normal["status"],
            "attempts": attempt_history(normal_attempts),
        },
        "retry": {
            "state": retry["status"],
            "attempts": attempt_history(retry_attempts),
        },
    }
    if json_output:
        print(json.dumps(result, indent=2))
        return result

    print("\nTaskForge Demo Complete\n" + "─" * 40)
    workers = {worker["id"]: worker for worker in client.workers()}
    _print_task_summary("NORMAL TASK", config, normal, normal_attempts, workers)
    _print_task_summary("RETRY TASK", config, retry, retry_attempts, workers)
    print(f"Web Console:  {config.web_url}")
    print(f"API Docs:     {config.api_docs_url}")
    print(f"Grafana:      {config.grafana_url}")
    print(f"Prometheus:   {config.prometheus_url}")
    print("\nNext: make demo-recovery")
    return result


def _print_task_summary(
    label: str,
    config: DemoConfig,
    task: dict[str, Any],
    attempts: list[dict[str, Any]],
    workers: dict[str, dict[str, Any]],
) -> None:
    history = " → ".join(attempt_history(attempts))
    print(f"\n{label}")
    print(f"Task ID:      {task['id']}")
    print(f"Handler:      {task['task_type']}")
    print(f"Final state:  {task['status']}")
    print(f"Attempts:     {len(attempts)} ({history})")
    for attempt in attempts:
        worker = workers.get(attempt["worker_id"], {})
        print(
            f"  #{attempt['attempt_number']} {attempt['status']:<10} "
            f"worker={worker.get('name', attempt['worker_id'])} "
            f"duration={attempt_duration(attempt)}"
        )
    print(f"Task detail:  {config.task_url(task['id'])}")


def run_demo_data(config: DemoConfig, client: APIClient) -> dict[str, Any]:
    print("TaskForge Demo Data\n" + "─" * 40)
    assert_demo_preconditions(config, client)
    identifier = run_id()
    created: list[tuple[dict[str, Any], dict[str, Any]]] = []
    specifications = demo_dataset()
    print(f"Creating {len(specifications)} real tasks (run {identifier})...")
    for specification in specifications:
        submission = {
            key: value
            for key, value in specification.items()
            if key not in {"label", "expected"}
        }
        task = client.submit(
            submission, f"tf014-data-{identifier}-{specification['label']}"
        )
        created.append((specification, task))
    deadline = time.monotonic() + 90
    for specification, submitted in created:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DemoError(
                "Demo dataset did not finish within the 90s overall timeout"
            )
        task = wait_for_task_status(
            client,
            submitted["id"],
            TERMINAL_TASK_STATES,
            timeout=remaining,
            progress=progress,
        )
        if task["status"] != specification["expected"]:
            raise DemoError(
                f"Demo data task {task['id']} expected {specification['expected']}, got {task['status']}"
            )
        if specification["label"].startswith("retry-"):
            validate_retry(task, client.attempts(task["id"]))
    states: dict[str, int] = {}
    for _, task in created:
        final = client.task(task["id"])
        states[final["status"]] = states.get(final["status"], 0) + 1
    print("\nDemo dataset complete  PASS")
    print(f"Run ID:       {identifier}")
    print(f"Tasks:        {len(created)}")
    print(
        "Final states: "
        + ", ".join(f"{key}={value}" for key, value in sorted(states.items()))
    )
    print(f"Open console: {config.web_url}/tasks")
    print(
        "Each run creates a new bounded dataset with unique tf014-data-* idempotency keys."
    )
    return {
        "result": "PASS",
        "run_id": identifier,
        "count": len(created),
        "states": states,
    }


def run_recovery(config: DemoConfig, client: APIClient) -> dict[str, Any]:
    print("Recovery Demo\n" + "─" * 40)
    print(
        "This demonstration will intentionally terminate one local TaskForge\n"
        "worker container to demonstrate lease-based recovery."
    )
    require_safe_local_docker(config)
    assert_demo_preconditions(config, client)
    identifier = run_id()
    submitted = client.submit(
        {
            "task_type": "test.sleep",
            "payload": {"duration_ms": 15000},
            "priority": 100,
            "max_attempts": 3,
        },
        f"tf014-recovery-{identifier}",
    )
    task_id = submitted["id"]
    running = wait_for_task_status(
        client, task_id, {"RUNNING"}, timeout=30, progress=progress
    )
    attempts = client.attempts(task_id)
    captured = next((item for item in attempts if item["status"] == "RUNNING"), None)
    if captured is None or running.get("claimed_by_worker_id") != captured.get(
        "worker_id"
    ):
        raise DemoError("RUNNING task did not expose one consistent owner attempt")
    worker, container = owner_container(config, client, captured["worker_id"])
    print(f"Task:             {task_id}")
    print(f"Attempt 1 owner:  {worker['name']} ({worker['id']})")
    print(f"Lease expires:    {running['lease_expires_at']}")
    print(f"Injecting failure: killing {container.name}")
    kill_and_restore(config, container)
    restored = container_running(config, container)

    recovery_timeout = (
        config.lease_seconds + (2 * config.recovery_interval_seconds) + 15
    )
    try:
        attempts = wait_for(
            lambda: client.attempts(task_id),
            lambda items: any(
                item["id"] == captured["id"] and item["status"] == "ABANDONED"
                for item in items
            ),
            description="lease expiration and scheduler recovery",
            timeout=recovery_timeout,
            progress=progress,
        )
        print("Captured attempt: ABANDONED")
        task = wait_for_task_status(
            client,
            task_id,
            TERMINAL_TASK_STATES,
            timeout=45,
            progress=progress,
        )
        attempts = client.attempts(task_id)
        validate_recovery(task, attempts, captured["id"])
    except DemoError as error:
        current = client.task(task_id)
        current_attempts = client.attempts(task_id)
        active_workers = sum(
            worker.get("liveness") == "ACTIVE" for worker in client.workers()
        )
        history = " → ".join(attempt_history(current_attempts)) or "none"
        raise DemoError(
            f"{error}\n"
            f"Task ID: {task_id}\n"
            f"Current task state: {current.get('status')}\n"
            f"Attempt history: {history}\n"
            f"Killed worker: {worker['name']} ({worker['id']})\n"
            f"Lease expiration: {running['lease_expires_at']}\n"
            f"Active workers: {active_workers}\n"
            f"Worker container restored: {'YES' if restored else 'NO'}"
        ) from error
    replacement = next(
        item
        for item in attempts
        if item["attempt_number"] > captured["attempt_number"]
        and item["status"] == "SUCCEEDED"
    )
    replacement_worker = next(
        (item for item in client.workers() if item["id"] == replacement["worker_id"]),
        None,
    )
    print("\nRecovery Demo Complete\n" + "─" * 40)
    print(f"Task ID:              {task_id}")
    print(f"Killed worker:         {worker['name']} ({worker['id']})")
    print(f"History:               {' → '.join(attempt_history(attempts))}")
    print(
        f"Replacement worker:    {(replacement_worker or {}).get('name', replacement['worker_id'])}"
    )
    print(f"Final state:           {task['status']}")
    print(f"Worker pool restored:  {'YES' if restored else 'NO — run make up'}")
    print(f"Task detail:           {config.task_url(task_id)}")
    return {
        "result": "PASS",
        "task_id": task_id,
        "killed_worker": worker["id"],
        "history": attempt_history(attempts),
        "replacement_worker": replacement["worker_id"],
        "worker_pool_restored": restored,
    }


def show_status(config: DemoConfig, client: APIClient) -> bool:
    probes = collect_status(config, client)
    print("TaskForge Development Status\n" + "─" * 40)
    for probe in probes:
        detail = f"  {probe.detail}" if probe.detail else ""
        print(f"{probe.label:16} {probe.state}{detail}")
    print("\nURLs")
    print(f"Web Console:  {config.web_url}")
    print(f"API:          {config.api_url}")
    print(f"API Docs:     {config.api_docs_url}")
    print(f"Grafana:      {config.grafana_url}")
    print(f"Prometheus:   {config.prometheus_url}")
    return all(probe.state == "READY" for probe in probes)


def wait_ready(config: DemoConfig, client: APIClient, timeout: float) -> None:
    started = time.monotonic()
    while True:
        probes = collect_status(config, client)
        if all(probe.state == "READY" for probe in probes):
            print("TaskForge is ready.")
            return
        elapsed = time.monotonic() - started
        if elapsed >= timeout:
            unavailable = ", ".join(
                f"{probe.label}={probe.state}"
                for probe in probes
                if probe.state != "READY"
            )
            raise DemoError(
                f"TaskForge did not become ready within {timeout:.0f}s: {unavailable}"
            )
        unavailable = ", ".join(
            probe.label for probe in probes if probe.state != "READY"
        )
        print(
            f"Waiting for TaskForge readiness ({unavailable})... {elapsed:.1f}s",
            flush=True,
        )
        time.sleep(2)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TaskForge local demonstrations")
    subparsers = parser.add_subparsers(dest="command", required=True)
    demo_parser = subparsers.add_parser("demo", help="run normal and retry scenarios")
    demo_parser.add_argument("--json", action="store_true")
    subparsers.add_parser("data", help="create a representative dataset")
    subparsers.add_parser("recovery", help="demonstrate local worker crash recovery")
    subparsers.add_parser("status", help="show local services and URLs")
    ready_parser = subparsers.add_parser(
        "wait-ready", help="wait for the complete stack"
    )
    ready_parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args(argv)
    config = resolve_config()
    client = APIClient(config.api_url)
    try:
        if args.command == "demo":
            run_demo(config, client, json_output=args.json)
        elif args.command == "data":
            run_demo_data(config, client)
        elif args.command == "recovery":
            run_recovery(config, client)
        elif args.command == "status":
            return 0 if show_status(config, client) else 1
        elif args.command == "wait-ready":
            wait_ready(config, client, args.timeout)
        return 0
    except (DemoError, ValueError) as error:
        print(f"\nFAIL — {error}", file=sys.stderr)
        if args.command == "recovery":
            print(
                "Debug with: make demo-status; docker compose ps; "
                "docker compose logs scheduler worker",
                file=sys.stderr,
            )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
