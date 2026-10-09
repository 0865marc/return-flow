from uuid import uuid4

import pytest

from application.errors import (
    BusinessRuleViolationError,
    ConcurrentModificationError,
    EntityNotFoundError,
)
from application.use_cases.deliveries import (
    CreateDelivery,
    GetDelivery,
    MarkDeliveryDelivered,
)
from domain import DeliveryStatus
from tests.application.fakes import InMemoryDeliveryRepository

pytestmark = pytest.mark.anyio


async def test_created_delivery_is_persisted_and_can_be_retrieved():
    repository = InMemoryDeliveryRepository()

    delivery = await CreateDelivery(repository).execute()
    retrieved = await GetDelivery(repository).execute(delivery.id)

    assert retrieved == delivery
    assert retrieved is not delivery
    assert retrieved.status == DeliveryStatus.PENDING


async def test_mark_delivered_persists_the_domain_transition():
    repository = InMemoryDeliveryRepository()
    delivery = await CreateDelivery(repository).execute()

    updated = await MarkDeliveryDelivered(repository).execute(delivery.id)

    assert updated.status == DeliveryStatus.DELIVERED
    assert await GetDelivery(repository).execute(delivery.id) == updated
    assert repository.save_count == 1


async def test_repeated_delivery_transition_does_not_save_again():
    repository = InMemoryDeliveryRepository()
    delivery = await CreateDelivery(repository).execute()
    use_case = MarkDeliveryDelivered(repository)
    await use_case.execute(delivery.id)

    with pytest.raises(BusinessRuleViolationError, match="Only pending deliveries"):
        await use_case.execute(delivery.id)

    assert repository.save_count == 1
    retrieved = await GetDelivery(repository).execute(delivery.id)
    assert retrieved.status == DeliveryStatus.DELIVERED


async def test_concurrent_delivery_transition_rejects_stale_write():
    class ConcurrentDeliveryRepository(InMemoryDeliveryRepository):
        async def get(self, delivery_id):
            delivery = await super().get(delivery_id)
            if delivery is not None and delivery.status == DeliveryStatus.PENDING:
                concurrent_delivery = delivery.model_copy(deep=True)
                concurrent_delivery.mark_delivered()
                await self.save(concurrent_delivery, expected_status=delivery.status)
            return delivery

    repository = ConcurrentDeliveryRepository()
    delivery = await CreateDelivery(repository).execute()

    with pytest.raises(ConcurrentModificationError, match=str(delivery.id)):
        await MarkDeliveryDelivered(repository).execute(delivery.id)

    assert repository.save_count == 1
    retrieved = await GetDelivery(repository).execute(delivery.id)
    assert retrieved.status == DeliveryStatus.DELIVERED


@pytest.mark.parametrize("use_case_type", [GetDelivery, MarkDeliveryDelivered])
async def test_missing_delivery_raises_not_found_without_saving(use_case_type):
    repository = InMemoryDeliveryRepository()
    delivery_id = uuid4()

    with pytest.raises(EntityNotFoundError, match=str(delivery_id)):
        await use_case_type(repository).execute(delivery_id)

    assert repository.deliveries == {}
    assert repository.save_count == 0


async def test_save_error_is_not_reported_as_a_business_rule_violation():
    class FailingDeliveryRepository(InMemoryDeliveryRepository):
        async def save(self, delivery, *, expected_status):
            raise ValueError("Persistence failed.")

    repository = FailingDeliveryRepository()
    delivery = await CreateDelivery(repository).execute()

    with pytest.raises(ValueError, match="Persistence failed") as error:
        await MarkDeliveryDelivered(repository).execute(delivery.id)

    assert not isinstance(error.value, BusinessRuleViolationError)
    retrieved = await GetDelivery(repository).execute(delivery.id)
    assert retrieved.status == DeliveryStatus.PENDING
