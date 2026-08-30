from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from app.liveness import WorkerLiveness


class TaskStatus(StrEnum):
    QUEUED = "QUEUED"
    LEASED = "LEASED"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TaskAttemptStatus(StrEnum):
    LEASED = "LEASED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    ABANDONED = "ABANDONED"


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
    queued_at: datetime
    claimed_by_worker_id: UUID | None
    lease_expires_at: datetime | None
    completed_at: datetime | None
    result: dict[str, Any] | None
    last_error: str | None
    idempotency_key: str | None
    request_fingerprint: str | None
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


@dataclass(frozen=True, slots=True)
class SubmitTaskResult:
    task: Task
    created: bool


@dataclass(frozen=True, slots=True)
class TaskAttempt:
    id: UUID
    task_id: UUID
    worker_id: UUID
    attempt_number: int
    status: TaskAttemptStatus
    leased_at: datetime
    queue_entered_at: datetime | None
    scheduled_at_snapshot: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    retry_scheduled_at: datetime | None
    recovered_lease_expires_at: datetime | None
    recovered_at: datetime | None
    recovery_action: str | None
    output: dict[str, Any] | None
    error: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class WorkerRecord:
    id: UUID
    instance_id: str
    name: str
    enabled: bool
    metadata: dict[str, Any]
    last_heartbeat: datetime | None
    registered_at: datetime
    updated_at: datetime
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class Worker:
    id: UUID
    instance_id: str
    name: str
    enabled: bool
    metadata: dict[str, Any]
    last_heartbeat: datetime | None
    registered_at: datetime
    updated_at: datetime
    liveness: WorkerLiveness
    heartbeat_age_seconds: float | None


@dataclass(frozen=True, slots=True)
class ExceptionalAttempt:
    task_id: UUID
    task_type: str
    attempt_number: int
    status: TaskAttemptStatus
    worker_id: UUID
    error: str | None
    retry_scheduled_at: datetime | None
    recovered_at: datetime | None
    recovery_action: str | None
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class OverviewSnapshot:
    task_counts: dict[TaskStatus, int]
    worker_counts: dict[WorkerLiveness, int]
    recent_tasks: list[Task]
    recent_exceptions: list[ExceptionalAttempt]
    observed_at: datetime
