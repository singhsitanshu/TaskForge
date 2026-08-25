from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID


class TaskStatus(StrEnum):
    QUEUED = "QUEUED"
    LEASED = "LEASED"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True, slots=True)
class Task:
    id: UUID
    queue: str
    task_type: str
    payload: dict[str, Any]
    status: TaskStatus
    priority: int
    max_attempts: int
    attempt_count: int
    scheduled_at: datetime
    leased_by_worker_id: UUID | None
    lease_expires_at: datetime | None
    completed_at: datetime | None
    result: dict[str, Any] | None
    last_error: str | None
    idempotency_key: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class NewTask:
    queue: str
    task_type: str
    payload: dict[str, Any]
    priority: int
    max_attempts: int
    scheduled_at: datetime | None
    idempotency_key: str | None
