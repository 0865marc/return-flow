from collections.abc import AsyncGenerator

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool

from adapters.persistence import postgres
from adapters.persistence.postgres import initialize_database
from application.ports.repositories import DeliveryRepository, ReturnRepository


@pytest.fixture(params=[False, True], ids=["transactional", "autocommit"])
def postgres_autocommit(request: pytest.FixtureRequest) -> bool:
    return bool(request.param)


@pytest.fixture
async def database_url(postgres_url: str) -> str:
    await initialize_database(postgres_url)
    return postgres_url


@pytest.fixture
async def database_pool(
    database_url: str, postgres_autocommit: bool,
) -> AsyncGenerator[AsyncConnectionPool[AsyncConnection[TupleRow]]]:
    pool = AsyncConnectionPool(
        database_url,
        connection_class=AsyncConnection[TupleRow],
        min_size=1,
        max_size=1,
        timeout=0.05,
        kwargs={"connect_timeout": 5, "autocommit": postgres_autocommit},
        open=False,
    )
    try:
        await pool.open(wait=True, timeout=5)
        yield pool
    finally:
        await pool.close()


@pytest.fixture
async def repositories(
    database_url: str, postgres_autocommit: bool,
) -> AsyncGenerator[tuple[DeliveryRepository, ReturnRepository]]:
    async with postgres.open_repositories(
        database_url, autocommit=postgres_autocommit
    ) as repositories:
        yield repositories.deliveries, repositories.returns
