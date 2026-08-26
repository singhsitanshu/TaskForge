from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status

from app.domain import TaskStatus
from app.schemas import (
    IdempotencyKey,
    TaskAttemptListResponse,
    TaskAttemptResponse,
    TaskCreate,
    TaskListResponse,
    TaskResponse,
)
from app.services import (
    IdempotencyKeyReuseError,
    TaskConflictError,
    TaskNotFoundError,
    TaskService,
)

router = APIRouter(prefix="/tasks", tags=["tasks"])


def get_task_service(request: Request) -> TaskService:
    return request.app.state.task_service


TaskServiceDependency = Annotated[TaskService, Depends(get_task_service)]


@router.post("", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
async def submit_task(
    request: TaskCreate,
    response: Response,
    service: TaskServiceDependency,
    idempotency_key_header: Annotated[
        IdempotencyKey | None,
        Header(alias="Idempotency-Key"),
    ] = None,
) -> TaskResponse:
    if (
        idempotency_key_header is not None
        and request.idempotency_key is not None
        and idempotency_key_header != request.idempotency_key
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "code": "IDEMPOTENCY_KEY_MISMATCH",
                "message": "header and deprecated body idempotency keys differ",
            },
        )
    idempotency_key = idempotency_key_header or request.idempotency_key
    try:
        result = await service.submit(request.to_domain(idempotency_key=idempotency_key))
    except IdempotencyKeyReuseError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "IDEMPOTENCY_KEY_REUSE",
                "message": "idempotency key was already used for another submission",
            },
        ) from exc
    response.status_code = status.HTTP_201_CREATED if result.created else status.HTTP_200_OK
    return TaskResponse.from_domain(result.task)


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


@router.get("/{task_id}/attempts", response_model=TaskAttemptListResponse)
async def list_task_attempts(
    task_id: UUID,
    service: TaskServiceDependency,
) -> TaskAttemptListResponse:
    try:
        attempts = await service.list_attempts(task_id)
    except TaskNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="task not found",
        ) from exc
    return TaskAttemptListResponse(
        items=[TaskAttemptResponse.from_domain(attempt) for attempt in attempts]
    )


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
