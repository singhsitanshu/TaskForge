from datetime import UTC, datetime, timedelta

import pytest

from app.config import HeartbeatSettings
from app.liveness import WorkerLiveness, classify_worker_liveness

SETTINGS = HeartbeatSettings(
    interval=timedelta(seconds=5),
    stale_after=timedelta(seconds=15),
    dead_after=timedelta(seconds=30),
)
OBSERVED_AT = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    ("age", "expected"),
    [
        (timedelta(0), WorkerLiveness.ACTIVE),
        (timedelta(seconds=15) - timedelta(microseconds=1), WorkerLiveness.ACTIVE),
        (timedelta(seconds=15), WorkerLiveness.ACTIVE),
        (timedelta(seconds=15) + timedelta(microseconds=1), WorkerLiveness.STALE),
        (timedelta(seconds=30), WorkerLiveness.STALE),
        (timedelta(seconds=30) + timedelta(microseconds=1), WorkerLiveness.DEAD),
    ],
)
def test_liveness_boundaries(age: timedelta, expected: WorkerLiveness) -> None:
    state, heartbeat_age = classify_worker_liveness(
        last_heartbeat=OBSERVED_AT - age,
        observed_at=OBSERVED_AT,
        settings=SETTINGS,
    )
    assert state is expected
    assert heartbeat_age == age.total_seconds()


def test_never_seen_worker_is_dead_with_unknown_age() -> None:
    assert classify_worker_liveness(
        last_heartbeat=None,
        observed_at=OBSERVED_AT,
        settings=SETTINGS,
    ) == (WorkerLiveness.DEAD, None)


@pytest.mark.parametrize(
    "configuration",
    [
        {"interval": timedelta(0)},
        {"stale_after": timedelta(seconds=5)},
        {"dead_after": timedelta(seconds=15)},
    ],
)
def test_invalid_liveness_configuration(configuration: dict) -> None:
    values = {
        "interval": timedelta(seconds=5),
        "stale_after": timedelta(seconds=15),
        "dead_after": timedelta(seconds=30),
    }
    values.update(configuration)
    with pytest.raises(ValueError):
        HeartbeatSettings(**values)


def test_duration_environment_parsing_and_validation() -> None:
    settings = HeartbeatSettings.from_env(
        {
            "WORKER_HEARTBEAT_INTERVAL": "250ms",
            "WORKER_STALE_AFTER": "1s",
            "WORKER_DEAD_AFTER": "2s",
        }
    )
    assert settings.interval == timedelta(milliseconds=250)
    assert settings.stale_after == timedelta(seconds=1)
    assert settings.dead_after == timedelta(seconds=2)

    with pytest.raises(ValueError, match="WORKER_STALE_AFTER"):
        HeartbeatSettings.from_env(
            {
                "WORKER_HEARTBEAT_INTERVAL": "1s",
                "WORKER_STALE_AFTER": "1s",
                "WORKER_DEAD_AFTER": "2s",
            }
        )
