from dataclasses import dataclass, field
from enum import StrEnum
from uuid import UUID, uuid4


class DeliveryStatus(StrEnum):
    PENDING = "pending"
    DELIVERED = "delivered"


@dataclass
class Delivery:
    id: UUID = field(default_factory=uuid4)
    status: DeliveryStatus = DeliveryStatus.PENDING

    def mark_delivered(self) -> None:
        if self.status != DeliveryStatus.PENDING:
            raise ValueError("Only pending deliveries can be marked as delivered.")
        self.status = DeliveryStatus.DELIVERED
