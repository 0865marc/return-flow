from dataclasses import dataclass, field
from enum import StrEnum
from uuid import UUID, uuid4

from .delivery import Delivery, DeliveryStatus


class ReturnStatus(StrEnum):
    REQUESTED = "requested"
    COMPLETED = "completed"


@dataclass
class Return:
    delivery_id: UUID
    id: UUID = field(default_factory=uuid4)
    status: ReturnStatus = ReturnStatus.REQUESTED

    @classmethod
    def request(cls, delivery: Delivery) -> "Return":
        if delivery.status != DeliveryStatus.DELIVERED:
            raise ValueError("A return can only be requested for a delivered delivery.")
        return cls(delivery_id=delivery.id)

    def complete(self) -> None:
        if self.status != ReturnStatus.REQUESTED:
            raise ValueError("Only requested returns can be completed.")
        self.status = ReturnStatus.COMPLETED
