from uuid import uuid4

import pytest
from psycopg.errors import ForeignKeyViolation

from adapters.persistence.postgres import initialize_database
from application.errors import ConcurrentModificationError, EntityNotFoundError
from domain.delivery import Delivery, DeliveryStatus
from domain.returns import Return

pytestmark = pytest.mark.integration


def test_delivery_can_be_created_loaded_and_updated(repositories) -> None:
    repository, _ = repositories
    delivery = Delivery()

    repository.add(delivery)
    assert repository.get(delivery.id) == delivery

    delivery.mark_delivered()
    assert repository.get(delivery.id).status == DeliveryStatus.PENDING

    repository.save(delivery, expected_status=DeliveryStatus.PENDING)
    assert repository.get(delivery.id) == delivery


def test_stale_delivery_cannot_overwrite_a_concurrent_update(repositories) -> None:
    repository, _ = repositories
    delivery = Delivery()
    repository.add(delivery)
    first_copy = repository.get(delivery.id)
    second_copy = repository.get(delivery.id)
    assert first_copy is not None
    assert second_copy is not None
    first_copy.mark_delivered()
    second_copy.mark_delivered()

    repository.save(first_copy, expected_status=DeliveryStatus.PENDING)

    with pytest.raises(ConcurrentModificationError):
        repository.save(second_copy, expected_status=DeliveryStatus.PENDING)

    assert repository.get(delivery.id) == first_copy


def test_return_can_be_created_and_loaded(repositories) -> None:
    deliveries, repository = repositories
    delivery = Delivery(status=DeliveryStatus.DELIVERED)
    deliveries.add(delivery)
    return_request = Return.request(delivery)

    repository.add(return_request)

    assert repository.get(return_request.id) == return_request


def test_unknown_ids_return_none(repositories) -> None:
    deliveries, returns = repositories
    assert deliveries.get(uuid4()) is None
    assert returns.get(uuid4()) is None


def test_save_does_not_create_a_missing_delivery(repositories) -> None:
    repository, _ = repositories
    delivery = Delivery()

    with pytest.raises(EntityNotFoundError):
        repository.save(delivery, expected_status=DeliveryStatus.PENDING)

    assert repository.get(delivery.id) is None


def test_initializing_again_preserves_deliveries_and_returns(
    database_url: str, repositories
) -> None:
    delivery = Delivery(status=DeliveryStatus.DELIVERED)
    return_request = Return.request(delivery)
    deliveries, returns = repositories
    deliveries.add(delivery)
    returns.add(return_request)

    initialize_database(database_url)

    assert deliveries.get(delivery.id) == delivery
    assert returns.get(return_request.id) == return_request


def test_return_requires_a_persisted_delivery(repositories) -> None:
    _, repository = repositories
    return_request = Return(delivery_id=uuid4())

    with pytest.raises(ForeignKeyViolation):
        repository.add(return_request)

    assert repository.get(return_request.id) is None
