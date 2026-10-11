from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from adapters.commands.contracts import CommandType
from domain.delivery import DeliveryStatus
from domain.returns import ReturnStatus


class DeliveryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    status: DeliveryStatus


class CommandAcceptedResponse(BaseModel):
    command_id: UUID
    status: Literal["accepted"] = "accepted"


class CommandResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    command_id: UUID
    type: CommandType
    status: Literal["pending", "completed", "failed"]
    result: dict[str, str] | None = None
    error: str | None = None


class RequestReturnBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    delivery_id: UUID


class ReturnResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    delivery_id: UUID
    status: ReturnStatus
