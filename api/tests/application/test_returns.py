from uuid import uuid4

import pytest

from application.errors import BusinessRuleViolationError, EntityNotFoundError
from application.use_cases.deliveries import (
    CreateDelivery,
    GetDelivery,
    MarkDeliveryDelivered,
)
from application.use_cases.returns import GetReturn, RequestReturn
from domain import DeliveryStatus, ReturnStatus
from tests.application.fakes import (
    InMemoryDeliveryRepository,
    InMemoryReturnRepository,
)

pytestmark = pytest.mark.anyio


async def test_delivered_delivery_can_be_returned_and_return_retrieved():
    deliveries = InMemoryDeliveryRepository()
    returns = InMemoryReturnRepository()
    delivery = await CreateDelivery(deliveries).execute()
    await MarkDeliveryDelivered(deliveries).execute(delivery.id)

    returned = await RequestReturn(deliveries, returns).execute(delivery.id)
    retrieved = await GetReturn(returns).execute(returned.id)

    assert retrieved == returned
    assert retrieved is not returned
    assert retrieved.delivery_id == delivery.id
    assert retrieved.status == ReturnStatus.REQUESTED
    retrieved_delivery = await GetDelivery(deliveries).execute(delivery.id)
    assert retrieved_delivery.status == DeliveryStatus.DELIVERED


async def test_pending_delivery_cannot_be_returned_and_nothing_is_persisted():
    deliveries = InMemoryDeliveryRepository()
    returns = InMemoryReturnRepository()
    delivery = await CreateDelivery(deliveries).execute()

    with pytest.raises(BusinessRuleViolationError, match="delivered delivery"):
        await RequestReturn(deliveries, returns).execute(delivery.id)

    assert returns.returns == {}
    retrieved = await GetDelivery(deliveries).execute(delivery.id)
    assert retrieved.status == DeliveryStatus.PENDING
    assert deliveries.save_count == 0


async def test_missing_delivery_cannot_be_returned_and_nothing_is_persisted():
    deliveries = InMemoryDeliveryRepository()
    returns = InMemoryReturnRepository()
    delivery_id = uuid4()

    with pytest.raises(EntityNotFoundError, match=str(delivery_id)):
        await RequestReturn(deliveries, returns).execute(delivery_id)

    assert returns.returns == {}
    assert deliveries.deliveries == {}


async def test_missing_return_raises_not_found():
    returns = InMemoryReturnRepository()
    return_id = uuid4()

    with pytest.raises(EntityNotFoundError, match=str(return_id)):
        await GetReturn(returns).execute(return_id)


async def test_repository_errors_are_not_reported_as_business_rule_violations():
    class FailingReturnRepository(InMemoryReturnRepository):
        async def add(self, returned):
            raise ValueError("Persistence failed.")

    deliveries = InMemoryDeliveryRepository()
    delivery = await CreateDelivery(deliveries).execute()
    await MarkDeliveryDelivered(deliveries).execute(delivery.id)

    with pytest.raises(ValueError, match="Persistence failed") as error:
        await RequestReturn(deliveries, FailingReturnRepository()).execute(delivery.id)

    assert not isinstance(error.value, BusinessRuleViolationError)
