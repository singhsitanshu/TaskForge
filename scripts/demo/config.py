from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_PORT_DEFAULTS = {
    "API_PORT": "8000",
    "WEB_PORT": "3000",
    "PROMETHEUS_PORT": "9090",
    "GRAFANA_PORT": "3001",
}


def read_environment(root: Path = ROOT) -> dict[str, str]:
    """Read Compose-style local values, with process environment taking precedence."""
    values: dict[str, str] = {}
    for path in (root / ".env.example", root / ".env"):
        if not path.exists():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    values.update(os.environ)
    return values


@dataclass(frozen=True)
class DemoConfig:
    root: Path
    api_url: str
    web_url: str
    prometheus_url: str
    grafana_url: str
    compose_project: str
    database_name: str
    database_user: str
    lease_seconds: float
    recovery_interval_seconds: float

    @property
    def api_docs_url(self) -> str:
        return f"{self.api_url}/docs"

    def task_url(self, task_id: str) -> str:
        return f"{self.web_url}/tasks/{task_id}"


def resolve_config(
    root: Path = ROOT, environment: dict[str, str] | None = None
) -> DemoConfig:
    values = read_environment(root) if environment is None else environment
    host = values.get("DEMO_HOST", "localhost")

    def url(name: str) -> str:
        explicit = values.get(f"DEMO_{name.removesuffix('_PORT')}_URL")
        if explicit:
            return explicit.rstrip("/")
        return f"http://{host}:{values.get(name, _PORT_DEFAULTS[name])}"

    return DemoConfig(
        root=root,
        api_url=url("API_PORT"),
        web_url=url("WEB_PORT"),
        prometheus_url=url("PROMETHEUS_PORT"),
        grafana_url=url("GRAFANA_PORT"),
        compose_project=values.get("COMPOSE_PROJECT_NAME", "taskforge"),
        database_name=values.get("POSTGRES_DB", "taskforge"),
        database_user=values.get("POSTGRES_USER", "taskforge"),
        lease_seconds=parse_go_duration(
            values.get("WORKER_TASK_LEASE_DURATION", "30s")
        ),
        recovery_interval_seconds=parse_go_duration(
            values.get("SCHEDULER_RECOVERY_INTERVAL", "5s")
        ),
    )


def parse_go_duration(value: str) -> float:
    units = {"h": 3600.0, "m": 60.0, "s": 1.0, "ms": 0.001, "us": 0.000001}
    position = 0
    total = 0.0
    for match in re.finditer(r"(\d+(?:\.\d+)?)(ms|us|h|m|s)", value):
        if match.start() != position:
            raise ValueError(f"unsupported duration: {value}")
        total += float(match.group(1)) * units[match.group(2)]
        position = match.end()
    if position != len(value) or total <= 0:
        raise ValueError(f"unsupported duration: {value}")
    return total
