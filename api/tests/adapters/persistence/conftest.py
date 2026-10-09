from collections.abc import Iterator

import pytest
from psycopg import Connection
from psycopg.rows import TupleRow
from psycopg_pool import ConnectionPool

from adapters.persistence.postgres import (
    PostgresDeliveryRepository,
    PostgresReturnRepository,
    initialize_database,
)
from adapters.persistence.postgres_pool import (
    PooledPostgresDeliveryRepository,
    PooledPostgresReturnRepository,
)
from application.ports.repositories import DeliveryRepository, ReturnRepository


@pytest.fixture
def database_url(postgres_url: str) -> str:
    initialize_database(postgres_url)
    return postgres_url


@pytest.fixture
def database_pool(database_url: str) -> Iterator[ConnectionPool]:
    with ConnectionPool(
        database_url,
        connection_class=Connection[TupleRow],
        min_size=1,
        max_size=1,
        timeout=0.05,
        kwargs={"connect_timeout": 5},
        open=False,
    ) as pool:
        pool.wait(timeout=5)
        yield pool


@pytest.fixture(params=["postgres", "postgres_pool"])
def repositories(
    database_url: str, request: pytest.FixtureRequest
) -> tuple[DeliveryRepository, ReturnRepository]:
    if request.param == "postgres":
        return (
            PostgresDeliveryRepository(database_url),
            PostgresReturnRepository(database_url),
        )

    pool = request.getfixturevalue("database_pool")
    return PooledPostgresDeliveryRepository(pool), PooledPostgresReturnRepository(pool)
