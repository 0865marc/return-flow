from typing import Protocol
from uuid import UUID

from domain import Delivery, Return


class DeliveryFlow(Protocol):
    """Input port shared by delivery and return request adapters."""

    async def create_delivery(self) -> Delivery: ...

    async def mark_delivered(self, delivery_id: UUID) -> Delivery: ...

    async def request_return(self, delivery_id: UUID) -> Return: ...
