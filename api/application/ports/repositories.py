from typing import Protocol
from uuid import UUID

from domain import Delivery, DeliveryStatus, Return


class DeliveryRepository(Protocol):
    def add(self, delivery: Delivery) -> None: ...

    def get(self, delivery_id: UUID) -> Delivery | None: ...

    def save(self, delivery: Delivery, *, expected_status: DeliveryStatus) -> None: ...


class ReturnRepository(Protocol):
    def add(self, returned: Return) -> None: ...

    def get(self, return_id: UUID) -> Return | None: ...
