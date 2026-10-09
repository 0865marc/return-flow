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


def test_created_delivery_is_persisted_and_can_be_retrieved():
    repository = InMemoryDeliveryRepository()

    delivery = CreateDelivery(repository).execute()
    retrieved = GetDelivery(repository).execute(delivery.id)

    assert retrieved == delivery
    assert retrieved is not delivery
    assert retrieved.status == DeliveryStatus.PENDING


def test_mark_delivered_persists_the_domain_transition():
    repository = InMemoryDeliveryRepository()
    delivery = CreateDelivery(repository).execute()

    updated = MarkDeliveryDelivered(repository).execute(delivery.id)

    assert updated.status == DeliveryStatus.DELIVERED
    assert GetDelivery(repository).execute(delivery.id) == updated
    assert repository.save_count == 1


def test_repeated_delivery_transition_does_not_save_again():
    repository = InMemoryDeliveryRepository()
    delivery = CreateDelivery(repository).execute()
    use_case = MarkDeliveryDelivered(repository)
    use_case.execute(delivery.id)

    with pytest.raises(BusinessRuleViolationError, match="Only pending deliveries"):
        use_case.execute(delivery.id)

    assert repository.save_count == 1
    assert GetDelivery(repository).execute(delivery.id).status == DeliveryStatus.DELIVERED


def test_concurrent_delivery_transition_rejects_stale_write():
    class ConcurrentDeliveryRepository(InMemoryDeliveryRepository):
        def get(self, delivery_id):
            delivery = super().get(delivery_id)
            if delivery is not None and delivery.status == DeliveryStatus.PENDING:
                concurrent_delivery = delivery.model_copy(deep=True)
                concurrent_delivery.mark_delivered()
                self.save(concurrent_delivery, expected_status=delivery.status)
            return delivery

    repository = ConcurrentDeliveryRepository()
    delivery = CreateDelivery(repository).execute()

    with pytest.raises(ConcurrentModificationError, match=str(delivery.id)):
        MarkDeliveryDelivered(repository).execute(delivery.id)

    assert repository.save_count == 1
    assert GetDelivery(repository).execute(delivery.id).status == DeliveryStatus.DELIVERED


@pytest.mark.parametrize("use_case_type", [GetDelivery, MarkDeliveryDelivered])
def test_missing_delivery_raises_not_found_without_saving(use_case_type):
    repository = InMemoryDeliveryRepository()
    delivery_id = uuid4()

    with pytest.raises(EntityNotFoundError, match=str(delivery_id)):
        use_case_type(repository).execute(delivery_id)

    assert repository.deliveries == {}
    assert repository.save_count == 0
