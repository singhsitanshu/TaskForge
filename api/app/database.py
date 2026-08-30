from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


def create_pool(
    database_url: str,
    connection_kwargs: dict[str, Any] | None = None,
) -> AsyncConnectionPool:
    kwargs: dict[str, Any] = {"row_factory": dict_row}
    kwargs.update(connection_kwargs or {})
    return AsyncConnectionPool(
        conninfo=database_url,
        kwargs=kwargs,
        min_size=1,
        max_size=10,
        open=False,
    )
