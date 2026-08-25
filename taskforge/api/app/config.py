import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta

_DURATION_PART = re.compile(r"(?P<value>\d+(?:\.\d+)?)(?P<unit>ms|s|m|h)")
_UNIT_SECONDS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}


def parse_duration(value: str, key: str) -> timedelta:
    position = 0
    seconds = 0.0
    for match in _DURATION_PART.finditer(value):
        if match.start() != position:
            break
        seconds += float(match.group("value")) * _UNIT_SECONDS[match.group("unit")]
        position = match.end()
    if position != len(value) or seconds <= 0:
        raise ValueError(f"{key} must be a positive duration such as 5s or 250ms")
    return timedelta(seconds=seconds)


@dataclass(frozen=True, slots=True)
class HeartbeatSettings:
    interval: timedelta = timedelta(seconds=5)
    stale_after: timedelta = timedelta(seconds=15)
    dead_after: timedelta = timedelta(seconds=30)

    def __post_init__(self) -> None:
        if self.interval <= timedelta(0):
            raise ValueError("WORKER_HEARTBEAT_INTERVAL must be positive")
        if self.stale_after <= self.interval:
            raise ValueError("WORKER_STALE_AFTER must be greater than WORKER_HEARTBEAT_INTERVAL")
        if self.dead_after <= self.stale_after:
            raise ValueError("WORKER_DEAD_AFTER must be greater than WORKER_STALE_AFTER")

    @classmethod
    def from_env(cls, environment: Mapping[str, str] | None = None) -> "HeartbeatSettings":
        values = environment if environment is not None else os.environ
        return cls(
            interval=parse_duration(
                values.get("WORKER_HEARTBEAT_INTERVAL", "5s"),
                "WORKER_HEARTBEAT_INTERVAL",
            ),
            stale_after=parse_duration(
                values.get("WORKER_STALE_AFTER", "15s"),
                "WORKER_STALE_AFTER",
            ),
            dead_after=parse_duration(
                values.get("WORKER_DEAD_AFTER", "30s"),
                "WORKER_DEAD_AFTER",
            ),
        )
