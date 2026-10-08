from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from domain import Delivery, DeliveryStatus


def test_pending_delivery_can_be_marked_delivered():
    delivery = Delivery()

    assert delivery.status == DeliveryStatus.PENDING

    delivery.mark_delivered()

    assert delivery.status == DeliveryStatus.DELIVERED


def test_delivered_delivery_cannot_be_marked_delivered_again():
    delivery = Delivery(status=DeliveryStatus.DELIVERED)

    with pytest.raises(ValueError):
        delivery.mark_delivered()

    assert delivery.status == DeliveryStatus.DELIVERED


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", "invalid-uuid"),
        ("status", "invalid-status"),
        ("unknown_field", "unexpected"),
    ],
)
def test_delivery_rejects_invalid_constructor_data(field, value):
    with pytest.raises(ValidationError):
        Delivery(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [("id", "invalid-uuid"), ("status", "invalid-status")],
)
def test_delivery_rejects_invalid_assignment_without_changing_value(field, value):
    delivery = Delivery()
    previous_value = getattr(delivery, field)

    with pytest.raises(ValidationError):
        setattr(delivery, field, value)

    assert getattr(delivery, field) == previous_value


def test_delivery_can_be_restored_from_serialized_values():
    delivery_id = uuid4()

    delivery = Delivery(id=str(delivery_id), status="delivered")

    assert isinstance(delivery.id, UUID)
    assert delivery.id == delivery_id
    assert delivery.status is DeliveryStatus.DELIVERED
