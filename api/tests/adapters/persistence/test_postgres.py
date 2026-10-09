from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.errors import ForeignKeyViolation
from psycopg.rows import TupleRow

from adapters.persistence.postgres import (
    PostgresDeliveryRepository,
    PostgresReturnRepository,
    initialize_database,
)
from application.errors import ConcurrentModificationError, EntityNotFoundError
from domain.delivery import Delivery, DeliveryStatus
from domain.returns import Return

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


async def test_delivery_can_be_created_loaded_and_updated(repositories) -> None:
    repository, _ = repositories
    delivery = Delivery()

    await repository.add(delivery)
    assert await repository.get(delivery.id) == delivery

    delivery.mark_delivered()
    stored = await repository.get(delivery.id)
    assert stored is not None and stored.status == DeliveryStatus.PENDING

    await repository.save(delivery, expected_status=DeliveryStatus.PENDING)
    assert await repository.get(delivery.id) == delivery


async def test_stale_delivery_cannot_overwrite_a_concurrent_update(repositories) -> None:
    repository, _ = repositories
    delivery = Delivery()
    await repository.add(delivery)
    first_copy = await repository.get(delivery.id)
    second_copy = await repository.get(delivery.id)
    assert first_copy is not None
    assert second_copy is not None
    first_copy.mark_delivered()
    second_copy.mark_delivered()

    await repository.save(first_copy, expected_status=DeliveryStatus.PENDING)

    with pytest.raises(ConcurrentModificationError):
        await repository.save(second_copy, expected_status=DeliveryStatus.PENDING)

    assert await repository.get(delivery.id) == first_copy


async def test_return_can_be_created_and_loaded(repositories) -> None:
    deliveries, repository = repositories
    delivery = Delivery(status=DeliveryStatus.DELIVERED)
    await deliveries.add(delivery)
    return_request = Return.request(delivery)

    await repository.add(return_request)

    assert await repository.get(return_request.id) == return_request


async def test_unknown_ids_return_none(repositories) -> None:
    deliveries, returns = repositories
    assert await deliveries.get(uuid4()) is None
    assert await returns.get(uuid4()) is None


async def test_save_does_not_create_a_missing_delivery(repositories) -> None:
    repository, _ = repositories
    delivery = Delivery()

    with pytest.raises(EntityNotFoundError):
        await repository.save(delivery, expected_status=DeliveryStatus.PENDING)

    assert await repository.get(delivery.id) is None


async def test_initializing_again_preserves_deliveries_and_returns(
    database_url: str, repositories
) -> None:
    delivery = Delivery(status=DeliveryStatus.DELIVERED)
    return_request = Return.request(delivery)
    deliveries, returns = repositories
    await deliveries.add(delivery)
    await returns.add(return_request)

    await initialize_database(database_url)

    assert await deliveries.get(delivery.id) == delivery
    assert await returns.get(return_request.id) == return_request


async def test_return_requires_a_persisted_delivery(repositories) -> None:
    _, repository = repositories
    return_request = Return(delivery_id=uuid4())

    with pytest.raises(ForeignKeyViolation):
        await repository.add(return_request)

    assert await repository.get(return_request.id) is None


async def test_each_operation_opens_and_closes_a_connection(
    database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    connections: list[AsyncConnection[TupleRow]] = []
    backends: list[int] = []
    original_connect = AsyncConnection.connect

    async def connect(conninfo: str, *, connect_timeout: int):
        connection = await original_connect(conninfo, connect_timeout=connect_timeout)
        connections.append(connection)
        backends.append(connection.info.backend_pid)
        return connection

    monkeypatch.setattr(AsyncConnection, "connect", staticmethod(connect))
    deliveries = PostgresDeliveryRepository(database_url)
    returns = PostgresReturnRepository(database_url)
    delivery = Delivery()

    await deliveries.add(delivery)
    assert connections[-1].closed
    assert await deliveries.get(delivery.id) == delivery
    assert connections[-1].closed
    delivery.mark_delivered()
    await deliveries.save(delivery, expected_status=DeliveryStatus.PENDING)
    assert connections[-1].closed

    returned = Return.request(delivery)
    await returns.add(returned)
    assert connections[-1].closed
    assert await returns.get(returned.id) == returned
    assert connections[-1].closed

    with pytest.raises(ForeignKeyViolation):
        await returns.add(Return(delivery_id=uuid4()))

    assert len(connections) == 6
    assert len(set(backends)) == 6
    assert all(connection.closed for connection in connections)
