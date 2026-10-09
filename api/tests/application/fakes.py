from uuid import UUID

from application.errors import ConcurrentModificationError, EntityNotFoundError
from domain import Delivery, DeliveryStatus, Return


class InMemoryDeliveryRepository:
    def __init__(self) -> None:
        self.deliveries: dict[UUID, Delivery] = {}
        self.save_count = 0

    def add(self, delivery: Delivery) -> None:
        self.deliveries[delivery.id] = delivery.model_copy(deep=True)

    def get(self, delivery_id: UUID) -> Delivery | None:
        delivery = self.deliveries.get(delivery_id)
        return delivery.model_copy(deep=True) if delivery is not None else None

    def save(self, delivery: Delivery, *, expected_status: DeliveryStatus) -> None:
        stored = self.deliveries.get(delivery.id)
        if stored is None:
            raise EntityNotFoundError(f"Delivery {delivery.id} was not found.")
        if stored.status != expected_status:
            raise ConcurrentModificationError(
                f"Delivery {delivery.id} changed before it could be saved."
            )
        self.save_count += 1
        self.deliveries[delivery.id] = delivery.model_copy(deep=True)


class InMemoryReturnRepository:
    def __init__(self) -> None:
        self.returns: dict[UUID, Return] = {}

    def add(self, returned: Return) -> None:
        self.returns[returned.id] = returned.model_copy(deep=True)

    def get(self, return_id: UUID) -> Return | None:
        returned = self.returns.get(return_id)
        return returned.model_copy(deep=True) if returned is not None else None
