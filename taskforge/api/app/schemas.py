from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from app.domain import NewTask, Task, TaskStatus

QueueName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=128),
]
TaskType = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
]
IdempotencyKey = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
]


class TaskCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_type: TaskType
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: QueueName = "default"
    priority: int = Field(default=0, ge=-32768, le=32767)
    max_attempts: int = Field(default=3, ge=1, le=100)
    scheduled_at: datetime | None = None
    idempotency_key: IdempotencyKey | None = None

    def to_domain(self) -> NewTask:
        return NewTask(
            queue=self.queue,
            task_type=self.task_type,
            payload=self.payload,
            priority=self.priority,
            max_attempts=self.max_attempts,
            scheduled_at=self.scheduled_at,
            idempotency_key=self.idempotency_key,
        )


class TaskResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

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

    @classmethod
    def from_domain(cls, task: Task) -> "TaskResponse":
        return cls.model_validate(task)


class TaskListResponse(BaseModel):
    items: list[TaskResponse]
    limit: int
    offset: int
