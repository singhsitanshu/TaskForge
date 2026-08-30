from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request

from app.schemas import OverviewResponse
from app.services import ConsoleService

router = APIRouter(prefix="/overview", tags=["console"])


def get_console_service(request: Request) -> ConsoleService:
    return request.app.state.console_service


ConsoleServiceDependency = Annotated[ConsoleService, Depends(get_console_service)]


@router.get("", response_model=OverviewResponse)
async def get_overview(
    service: ConsoleServiceDependency,
    recent_limit: Annotated[int, Query(ge=1, le=25)] = 8,
) -> OverviewResponse:
    return OverviewResponse.from_domain(await service.overview(recent_limit=recent_limit))
