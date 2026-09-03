from __future__ import annotations

import json
import os
import subprocess
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from .client import APIClient, DemoError
from .config import DemoConfig
from .scenarios import Container, select_owner_container


@dataclass(frozen=True)
class Probe:
    label: str
    state: str
    detail: str = ""


def run_command(
    config: DemoConfig, arguments: list[str], *, check: bool = False
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            arguments,
            cwd=config.root,
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError as error:
        raise DemoError(
            f"Required command {arguments[0]!r} was not found; install Docker with Compose"
        ) from error
    if check and result.returncode != 0:
        message = (result.stderr or result.stdout).strip()
        raise DemoError(f"Command failed: {' '.join(arguments)}\n{message}")
    return result


def http_probe(label: str, endpoint: str) -> Probe:
    try:
        with urllib.request.urlopen(endpoint, timeout=3) as response:
            if 200 <= response.status < 300:
                return Probe(label, "READY")
            return Probe(label, "NOT READY", f"HTTP {response.status}")
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        detail = str(error.reason if hasattr(error, "reason") else error)
        return Probe(label, "NOT READY", detail)


def command_probe(
    config: DemoConfig, label: str, arguments: list[str], expected: str = ""
) -> Probe:
    result = run_command(config, arguments)
    output = (result.stdout or result.stderr).strip()
    if result.returncode == 0 and (not expected or expected == output):
        return Probe(label, "READY")
    return Probe(label, "NOT READY", output[:160] or f"exit {result.returncode}")


def collect_status(config: DemoConfig, client: APIClient | None = None) -> list[Probe]:
    client = client or APIClient(config.api_url)
    probes = [
        http_probe("Web Console", f"{config.web_url}/healthz"),
        http_probe("API", f"{config.api_url}/readyz"),
        command_probe(
            config,
            "PostgreSQL",
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "postgres",
                "pg_isready",
                "--username",
                config.database_user,
                "--dbname",
                config.database_name,
            ],
        ),
        command_probe(
            config,
            "Redis",
            ["docker", "compose", "exec", "-T", "redis", "redis-cli", "ping"],
            "PONG",
        ),
        command_probe(
            config,
            "Scheduler",
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "scheduler",
                "wget",
                "--quiet",
                "--tries=1",
                "--spider",
                "http://127.0.0.1:8080/readyz",
            ],
        ),
        http_probe("Prometheus", f"{config.prometheus_url}/-/ready"),
        http_probe("Grafana", f"{config.grafana_url}/api/health"),
    ]
    try:
        workers = client.workers()
        active = [worker for worker in workers if worker.get("liveness") == "ACTIVE"]
        probes.insert(
            5,
            Probe(
                "Workers", "READY" if active else "NOT READY", f"{len(active)} ACTIVE"
            ),
        )
    except DemoError as error:
        probes.insert(5, Probe("Workers", "UNKNOWN", str(error)))
    return probes


def assert_demo_preconditions(config: DemoConfig, client: APIClient) -> None:
    probes = collect_status(config, client)
    required = {"API", "PostgreSQL", "Scheduler", "Workers"}
    failed = [
        probe for probe in probes if probe.label in required and probe.state != "READY"
    ]
    if failed:
        lines = ["TaskForge demo cannot start.", ""]
        for probe in probes:
            if probe.label in required:
                suffix = f" ({probe.detail})" if probe.detail else ""
                lines.append(f"{probe.label + ':':12} {probe.state}{suffix}")
        lines.extend(
            ["", "Start the complete local stack and retry:", "", "    make up"]
        )
        raise DemoError("\n".join(lines))


def require_safe_local_docker(config: DemoConfig) -> None:
    hostname = urlparse(config.api_url).hostname
    if hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise DemoError(
            f"Recovery failure injection refused: API host {hostname!r} is not local"
        )
    if os.environ.get("DOCKER_HOST", "").startswith(("tcp://", "ssh://")):
        raise DemoError("Recovery failure injection refused: DOCKER_HOST is remote")
    context = (
        run_command(
            config,
            [
                "docker",
                "context",
                "inspect",
                "--format",
                "{{json .Endpoints.docker.Host}}",
            ],
            check=True,
        )
        .stdout.strip()
        .strip('"')
    )
    if not context.startswith(("unix://", "npipe://")):
        raise DemoError(
            f"Recovery failure injection refused: Docker endpoint is {context!r}"
        )


def worker_containers(config: DemoConfig) -> list[Container]:
    listed = run_command(
        config, ["docker", "compose", "ps", "-a", "-q", "worker"], check=True
    ).stdout.split()
    containers = []
    for container_id in listed:
        inspected = run_command(
            config,
            [
                "docker",
                "inspect",
                "--format",
                "{{json .Id}} {{json .Config.Hostname}} {{json .Name}} {{json .HostConfig.RestartPolicy.Name}}",
                container_id,
            ],
            check=True,
        ).stdout.strip()
        decoder = json.JSONDecoder()
        values: list[str] = []
        remaining = inspected
        for _ in range(4):
            value, index = decoder.raw_decode(remaining.lstrip())
            values.append(value)
            remaining = remaining.lstrip()[index:]
        containers.append(
            Container(values[0], values[1], values[2].lstrip("/"), values[3])
        )
    return containers


def owner_container(
    config: DemoConfig, client: APIClient, worker_id: str
) -> tuple[dict[str, Any], Container]:
    worker = next(
        (item for item in client.workers() if item.get("id") == worker_id), None
    )
    if worker is None:
        raise DemoError(
            f"Owner worker {worker_id} was not returned by the TaskForge API"
        )
    container = select_owner_container(worker, worker_containers(config))
    labels = run_command(
        config,
        [
            "docker",
            "inspect",
            "--format",
            '{{index .Config.Labels "com.docker.compose.project"}}/{{index .Config.Labels "com.docker.compose.service"}}',
            container.container_id,
        ],
        check=True,
    ).stdout.strip()
    if labels != f"{config.compose_project}/worker":
        raise DemoError(
            f"Recovery failure injection refused: owner container labels are {labels!r}"
        )
    return worker, container


def kill_and_restore(config: DemoConfig, container: Container) -> None:
    policy = container.restart_policy or "no"
    run_command(
        config, ["docker", "update", "--restart=no", container.container_id], check=True
    )
    try:
        run_command(
            config,
            ["docker", "kill", "--signal", "KILL", container.container_id],
            check=True,
        )
    finally:
        run_command(
            config,
            ["docker", "update", f"--restart={policy}", container.container_id],
            check=True,
        )
    run_command(config, ["docker", "start", container.container_id], check=True)


def container_running(config: DemoConfig, container: Container) -> bool:
    result = run_command(
        config,
        ["docker", "inspect", "--format", "{{.State.Running}}", container.container_id],
    )
    return result.returncode == 0 and result.stdout.strip() == "true"
