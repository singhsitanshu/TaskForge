from datetime import datetime
from enum import StrEnum

from app.config import HeartbeatSettings


class WorkerLiveness(StrEnum):
    ACTIVE = "ACTIVE"
    STALE = "STALE"
    DEAD = "DEAD"


def classify_worker_liveness(
    *,
    last_heartbeat: datetime | None,
    observed_at: datetime,
    settings: HeartbeatSettings,
) -> tuple[WorkerLiveness, float | None]:
    if last_heartbeat is None:
        return WorkerLiveness.DEAD, None

    heartbeat_age_seconds = max(
        0.0,
        (observed_at - last_heartbeat).total_seconds(),
    )
    if heartbeat_age_seconds <= settings.stale_after.total_seconds():
        return WorkerLiveness.ACTIVE, heartbeat_age_seconds
    if heartbeat_age_seconds <= settings.dead_after.total_seconds():
        return WorkerLiveness.STALE, heartbeat_age_seconds
    return WorkerLiveness.DEAD, heartbeat_age_seconds
