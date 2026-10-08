from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class DeliveryStatus(StrEnum):
    PENDING = "pending"
    DELIVERED = "delivered"


class Delivery(BaseModel):
    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    id: UUID = Field(default_factory=uuid4)
    status: DeliveryStatus = DeliveryStatus.PENDING

    def mark_delivered(self) -> None:
        if self.status != DeliveryStatus.PENDING:
            raise ValueError("Only pending deliveries can be marked as delivered.")
        self.status = DeliveryStatus.DELIVERED
