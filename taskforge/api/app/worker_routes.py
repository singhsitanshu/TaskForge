from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from app.schemas import WorkerListResponse, WorkerResponse
from app.services import WorkerNotFoundError, WorkerService

router = APIRouter(prefix="/workers", tags=["workers"])


def get_worker_service(request: Request) -> WorkerService:
    return request.app.state.worker_service


WorkerServiceDependency = Annotated[WorkerService, Depends(get_worker_service)]


@router.get("/{worker_id}", response_model=WorkerResponse)
async def get_worker(
    worker_id: UUID,
    service: WorkerServiceDependency,
) -> WorkerResponse:
    try:
        worker = await service.get(worker_id)
    except WorkerNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="worker not found",
        ) from exc
    return WorkerResponse.from_domain(worker)


@router.get("", response_model=WorkerListResponse)
async def list_workers(
    service: WorkerServiceDependency,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> WorkerListResponse:
    workers, total = await service.list(limit=limit, offset=offset)
    return WorkerListResponse(
        items=[WorkerResponse.from_domain(worker) for worker in workers],
        limit=limit,
        offset=offset,
        total=total,
    )
