import hashlib
import json
from datetime import UTC, datetime

from app.domain import NewTask


def submission_fingerprint(task: NewTask) -> str:
    canonical_submission = {
        "max_attempts": task.max_attempts,
        "payload": task.payload,
        "priority": task.priority,
        "queue": task.queue,
        "scheduled_at": _canonical_datetime(task.scheduled_at),
        "task_type": task.task_type,
    }
    encoded = json.dumps(
        canonical_submission,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def idempotency_key_hash(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def _canonical_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="microseconds")
