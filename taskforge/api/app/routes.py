from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from app.domain import TaskStatus
from app.repositories import DuplicateTaskError
from app.schemas import TaskCreate, TaskListResponse, TaskResponse
from app.services import TaskConflictError, TaskNotFoundError, TaskService

router = APIRouter(prefix="/tasks", tags=["tasks"])


def get_task_service(request: Request) -> TaskService:
    return request.app.state.task_service


TaskServiceDependency = Annotated[TaskService, Depends(get_task_service)]


@router.post("", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
async def submit_task(
    request: TaskCreate,
    service: TaskServiceDependency,
) -> TaskResponse:
    try:
        task = await service.submit(request.to_domain())
    except DuplicateTaskError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="a task with this queue and idempotency key already exists",
        ) from exc
    return TaskResponse.from_domain(task)


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: UUID,
    service: TaskServiceDependency,
) -> TaskResponse:
    try:
        task = await service.get(task_id)
    except TaskNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="task not found",
        ) from exc
    return TaskResponse.from_domain(task)


@router.get("", response_model=TaskListResponse)
async def list_tasks(
    service: TaskServiceDependency,
    task_status: Annotated[TaskStatus | None, Query(alias="status")] = None,
    queue: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> TaskListResponse:
    tasks = await service.list(
        status=task_status,
        queue=queue,
        limit=limit,
        offset=offset,
    )
    return TaskListResponse(
        items=[TaskResponse.from_domain(task) for task in tasks],
        limit=limit,
        offset=offset,
    )


@router.post("/{task_id}/cancel", response_model=TaskResponse)
async def cancel_task(
    task_id: UUID,
    service: TaskServiceDependency,
) -> TaskResponse:
    try:
        task = await service.cancel(task_id)
    except TaskNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="task not found",
        ) from exc
    except TaskConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    return TaskResponse.from_domain(task)
