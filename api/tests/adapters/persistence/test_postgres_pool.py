from uuid import uuid4

import psycopg
import pytest
from psycopg.errors import ForeignKeyViolation
from psycopg.pq import TransactionStatus
from psycopg_pool import ConnectionPool, PoolTimeout

from adapters.persistence.postgres_pool import (
    PooledPostgresDeliveryRepository,
    PooledPostgresReturnRepository,
    open_repositories,
)
from domain.delivery import Delivery, DeliveryStatus
from domain.returns import Return

pytestmark = pytest.mark.integration


def test_repositories_reuse_connection_and_commit_writes(
    database_url: str, database_pool: ConnectionPool
) -> None:
    deliveries = PooledPostgresDeliveryRepository(database_pool)
    returns = PooledPostgresReturnRepository(database_pool)
    with database_pool.connection() as connection:
        original_backend = connection.info.backend_pid

    delivery = Delivery()
    deliveries.add(delivery)
    delivery.mark_delivered()
    deliveries.save(delivery, expected_status=DeliveryStatus.PENDING)
    returned = Return.request(delivery)
    returns.add(returned)

    assert deliveries.get(delivery.id) == delivery
    assert returns.get(returned.id) == returned
    with psycopg.connect(database_url) as connection:
        assert connection.execute(
            "SELECT status FROM deliveries WHERE id = %s", (delivery.id,)
        ).fetchone() == ("delivered",)
        assert connection.execute(
            "SELECT delivery_id, status FROM returns WHERE id = %s", (returned.id,)
        ).fetchone() == (delivery.id, "requested")

    with database_pool.connection() as connection:
        assert connection.info.backend_pid == original_backend
        assert connection.info.transaction_status == TransactionStatus.IDLE


def test_failed_write_rolls_back_and_connection_can_be_reused(
    database_url: str, database_pool: ConnectionPool
) -> None:
    deliveries = PooledPostgresDeliveryRepository(database_pool)
    returns = PooledPostgresReturnRepository(database_pool)
    with database_pool.connection() as connection:
        original_backend = connection.info.backend_pid

    with pytest.raises(ForeignKeyViolation):
        returns.add(Return(delivery_id=uuid4()))

    delivery = Delivery(status=DeliveryStatus.DELIVERED)
    deliveries.add(delivery)
    returned = Return.request(delivery)
    returns.add(returned)
    assert returns.get(returned.id) == returned
    with psycopg.connect(database_url) as connection:
        assert connection.execute("SELECT COUNT(*) FROM returns").fetchone() == (1,)

    with database_pool.connection() as connection:
        assert connection.info.backend_pid == original_backend
        assert connection.info.transaction_status == TransactionStatus.IDLE


def test_exhausted_pool_times_out_and_recovers(database_pool: ConnectionPool) -> None:
    deliveries = PooledPostgresDeliveryRepository(database_pool)

    with database_pool.connection():
        with pytest.raises(PoolTimeout):
            deliveries.get(uuid4())

    delivery = Delivery()
    deliveries.add(delivery)
    assert deliveries.get(delivery.id) == delivery


def test_repository_context_closes_shared_pool_when_operation_fails(
    postgres_url: str,
) -> None:
    pool = None
    connection = None
    with pytest.raises(RuntimeError, match="Operation failed"):
        with open_repositories(postgres_url) as repositories:
            assert isinstance(repositories.deliveries, PooledPostgresDeliveryRepository)
            assert isinstance(repositories.returns, PooledPostgresReturnRepository)
            pool = repositories.deliveries.pool
            assert repositories.returns.pool is pool
            with pool.connection() as connection:
                assert not connection.closed

            raise RuntimeError("Operation failed")

    assert pool is not None and pool.closed
    assert connection is not None and connection.closed
