from uuid import UUID

from pydantic import BaseModel, ConfigDict

from domain.delivery import DeliveryStatus
from domain.returns import ReturnStatus


class DeliveryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    status: DeliveryStatus


class RequestReturnBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    delivery_id: UUID


class ReturnResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    delivery_id: UUID
    status: ReturnStatus
