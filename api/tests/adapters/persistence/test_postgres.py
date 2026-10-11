import asyncio
from uuid import uuid4

import pytest
from psycopg.errors import ForeignKeyViolation

from adapters.persistence.postgres import (
    initialize_database,
    open_repositories,
)
from application.errors import ConcurrentModificationError, EntityNotFoundError
from domain.delivery import Delivery, DeliveryStatus
from domain.returns import Return

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


async def test_concurrent_api_and_worker_initialization_is_safe(postgres_url) -> None:
    await asyncio.gather(*(initialize_database(postgres_url) for _ in range(8)))
    async with open_repositories(postgres_url) as repositories:
        delivery = Delivery()
        await repositories.deliveries.add(delivery)
        assert await repositories.deliveries.get(delivery.id) == delivery


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
