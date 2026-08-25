from collections.abc import Sequence
from uuid import UUID

from app.domain import NewTask, Task, TaskStatus
from app.repositories import DuplicateTaskError, TaskRepository


class TaskNotFoundError(Exception):
    pass


class TaskConflictError(Exception):
    def __init__(self, status: TaskStatus) -> None:
        self.status = status
        super().__init__(f"task in {status} state cannot be cancelled")


class TaskService:
    def __init__(self, repository: TaskRepository) -> None:
        self._repository = repository

    async def submit(self, new_task: NewTask) -> Task:
        return await self._repository.create(new_task)

    async def get(self, task_id: UUID) -> Task:
        task = await self._repository.get(task_id)
        if task is None:
            raise TaskNotFoundError
        return task

    async def list(
        self,
        *,
        status: TaskStatus | None,
        queue: str | None,
        limit: int,
        offset: int,
    ) -> Sequence[Task]:
        return await self._repository.list(
            status=status,
            queue=queue,
            limit=limit,
            offset=offset,
        )

    async def cancel(self, task_id: UUID) -> Task:
        cancelled = await self._repository.cancel_active(task_id)
        if cancelled is not None:
            return cancelled

        task = await self._repository.get(task_id)
        if task is None:
            raise TaskNotFoundError
        if task.status is TaskStatus.CANCELLED:
            return task
        raise TaskConflictError(task.status)


__all__ = [
    "DuplicateTaskError",
    "TaskConflictError",
    "TaskNotFoundError",
    "TaskService",
]
