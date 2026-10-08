from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from domain import Delivery, DeliveryStatus, Return, ReturnStatus


def test_delivered_delivery_can_be_returned_and_completed():
    delivery = Delivery(status=DeliveryStatus.DELIVERED)

    returned = Return.request(delivery)

    assert returned.delivery_id == delivery.id
    assert returned.status == ReturnStatus.REQUESTED

    returned.complete()

    assert returned.status == ReturnStatus.COMPLETED
    assert delivery.status == DeliveryStatus.DELIVERED


def test_pending_delivery_cannot_be_returned():
    delivery = Delivery()

    with pytest.raises(ValueError):
        Return.request(delivery)

    assert delivery.status == DeliveryStatus.PENDING


def test_completed_return_cannot_be_completed_again():
    delivery_id = uuid4()
    returned = Return(delivery_id=delivery_id, status=ReturnStatus.COMPLETED)

    with pytest.raises(ValueError):
        returned.complete()

    assert returned.status == ReturnStatus.COMPLETED
    assert returned.delivery_id == delivery_id


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", "invalid-uuid"),
        ("delivery_id", "invalid-uuid"),
        ("status", "invalid-status"),
        ("unknown_field", "unexpected"),
    ],
)
def test_return_rejects_invalid_constructor_data(field, value):
    data = {"delivery_id": uuid4(), field: value}

    with pytest.raises(ValidationError):
        Return(**data)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", "invalid-uuid"),
        ("delivery_id", "invalid-uuid"),
        ("status", "invalid-status"),
    ],
)
def test_return_rejects_invalid_assignment_without_changing_value(field, value):
    returned = Return(delivery_id=uuid4())
    previous_value = getattr(returned, field)

    with pytest.raises(ValidationError):
        setattr(returned, field, value)

    assert getattr(returned, field) == previous_value


def test_return_can_be_restored_from_serialized_values():
    return_id = uuid4()
    delivery_id = uuid4()

    returned = Return(
        id=str(return_id), delivery_id=str(delivery_id), status="completed"
    )

    assert isinstance(returned.id, UUID)
    assert returned.id == return_id
    assert isinstance(returned.delivery_id, UUID)
    assert returned.delivery_id == delivery_id
    assert returned.status is ReturnStatus.COMPLETED
