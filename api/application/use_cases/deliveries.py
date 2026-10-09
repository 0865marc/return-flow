from uuid import UUID

from application.errors import BusinessRuleViolationError, EntityNotFoundError
from application.ports.repositories import DeliveryRepository
from domain import Delivery


class CreateDelivery:
    def __init__(self, repository: DeliveryRepository) -> None:
        self.repository = repository

    def execute(self) -> Delivery:
        delivery = Delivery()
        self.repository.add(delivery)
        return delivery


class GetDelivery:
    def __init__(self, repository: DeliveryRepository) -> None:
        self.repository = repository

    def execute(self, delivery_id: UUID) -> Delivery:
        delivery = self.repository.get(delivery_id)
        if delivery is None:
            raise EntityNotFoundError(f"Delivery {delivery_id} was not found.")
        return delivery


class MarkDeliveryDelivered:
    def __init__(self, repository: DeliveryRepository) -> None:
        self.repository = repository

    def execute(self, delivery_id: UUID) -> Delivery:
        delivery = GetDelivery(self.repository).execute(delivery_id)
        expected_status = delivery.status
        try:
            delivery.mark_delivered()
        except ValueError as error:
            raise BusinessRuleViolationError(str(error)) from error
        self.repository.save(delivery, expected_status=expected_status)
        return delivery
