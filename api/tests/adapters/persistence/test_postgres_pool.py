from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.errors import ForeignKeyViolation
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from adapters.persistence import postgres_pool
from adapters.persistence.postgres_pool import (
    PooledPostgresDeliveryRepository,
    PooledPostgresReturnRepository,
    open_repositories,
)
from domain.delivery import Delivery, DeliveryStatus
from domain.returns import Return

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


async def test_repositories_reuse_connection_and_commit_writes(
    database_url: str, database_pool
) -> None:
    deliveries = PooledPostgresDeliveryRepository(database_pool)
    returns = PooledPostgresReturnRepository(database_pool)
    async with database_pool.connection() as connection:
        original_backend = connection.info.backend_pid

    delivery = Delivery()
    await deliveries.add(delivery)
    delivery.mark_delivered()
    await deliveries.save(delivery, expected_status=DeliveryStatus.PENDING)
    returned = Return.request(delivery)
    await returns.add(returned)

    assert await deliveries.get(delivery.id) == delivery
    assert await returns.get(returned.id) == returned
    async with await AsyncConnection.connect(database_url) as connection:
        cursor = await connection.execute(
            "SELECT status FROM deliveries WHERE id = %s", (delivery.id,)
        )
        assert await cursor.fetchone() == ("delivered",)
        cursor = await connection.execute(
            "SELECT delivery_id, status FROM returns WHERE id = %s", (returned.id,)
        )
        assert await cursor.fetchone() == (delivery.id, "requested")

    async with database_pool.connection() as connection:
        assert connection.info.backend_pid == original_backend
        assert connection.info.transaction_status == TransactionStatus.IDLE


async def test_failed_write_rolls_back_and_connection_can_be_reused(
    database_url: str, database_pool
) -> None:
    deliveries = PooledPostgresDeliveryRepository(database_pool)
    returns = PooledPostgresReturnRepository(database_pool)
    async with database_pool.connection() as connection:
        original_backend = connection.info.backend_pid

    rejected = Return(delivery_id=uuid4())
    with pytest.raises(ForeignKeyViolation):
        await returns.add(rejected)

    assert await returns.get(rejected.id) is None
    delivery = Delivery(status=DeliveryStatus.DELIVERED)
    await deliveries.add(delivery)
    returned = Return.request(delivery)
    await returns.add(returned)
    assert await returns.get(returned.id) == returned
    async with await AsyncConnection.connect(database_url) as connection:
        cursor = await connection.execute("SELECT COUNT(*) FROM returns")
        assert await cursor.fetchone() == (1,)

    async with database_pool.connection() as connection:
        assert connection.info.backend_pid == original_backend
        assert connection.info.transaction_status == TransactionStatus.IDLE


async def test_exhausted_pool_times_out_and_recovers(database_pool) -> None:
    deliveries = PooledPostgresDeliveryRepository(database_pool)

    async with database_pool.connection():
        with pytest.raises(PoolTimeout):
            await deliveries.get(uuid4())

    delivery = Delivery()
    await deliveries.add(delivery)
    assert await deliveries.get(delivery.id) == delivery


async def test_repository_context_closes_shared_pool_when_operation_fails(
    postgres_url: str,
) -> None:
    pool = None
    connection = None
    with pytest.raises(RuntimeError, match="Operation failed"):
        async with open_repositories(postgres_url) as repositories:
            assert isinstance(
                repositories.deliveries, PooledPostgresDeliveryRepository
            )
            assert isinstance(repositories.returns, PooledPostgresReturnRepository)
            pool = repositories.deliveries.pool
            assert repositories.returns.pool is pool
            async with pool.connection() as connection:
                assert not connection.closed

            raise RuntimeError("Operation failed")

    assert pool is not None and pool.closed
    assert connection is not None and connection.closed


async def test_repository_context_closes_pool_when_startup_fails(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    pools: list[AsyncConnectionPool[AsyncConnection[TupleRow]]] = []
    connections: list[AsyncConnection[TupleRow]] = []
    original_open = AsyncConnectionPool.open

    async def fail_after_open(
        pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
        wait: bool = False,
        timeout: float = 30.0,
    ) -> None:
        pools.append(pool)
        await original_open(pool, wait=wait, timeout=timeout)
        async with pool.connection() as connection:
            connections.append(connection)
        raise RuntimeError("Startup failed")

    monkeypatch.setattr(postgres_pool.AsyncConnectionPool, "open", fail_after_open)

    with pytest.raises(RuntimeError, match="Startup failed"):
        async with open_repositories(postgres_url):
            pytest.fail("Repositories must not be provided after startup failure")

    assert len(pools) == 1 and pools[0].closed
    assert len(connections) == 1 and connections[0].closed
