from typing import Protocol
from uuid import UUID

from domain import Delivery, DeliveryStatus, Return


class DeliveryRepository(Protocol):
    async def add(self, delivery: Delivery) -> None: ...

    async def get(self, delivery_id: UUID) -> Delivery | None: ...

    async def save(
        self, delivery: Delivery, *, expected_status: DeliveryStatus
    ) -> None: ...


class ReturnRepository(Protocol):
    async def add(self, returned: Return) -> None: ...

    async def get(self, return_id: UUID) -> Return | None: ...
