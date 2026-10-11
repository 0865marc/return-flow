"""Fictitious DHL payloads, independent of HTTP or messaging."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from domain import Delivery, DeliveryStatus, Return, ReturnStatus

DHLDeliveryStatus = Literal["PENDING", "DELIVERED"]
DHLReturnStatus = Literal["REQUESTED", "COMPLETED"]

_DELIVERY_STATUSES: dict[DeliveryStatus, DHLDeliveryStatus] = {
    DeliveryStatus.PENDING: "PENDING",
    DeliveryStatus.DELIVERED: "DELIVERED",
}
_RETURN_STATUSES: dict[ReturnStatus, DHLReturnStatus] = {
    ReturnStatus.REQUESTED: "REQUESTED",
    ReturnStatus.COMPLETED: "COMPLETED",
}


class DHLDeliveryResponse(BaseModel):
    shipment_id: UUID = Field(alias="shipmentId")
    status: DHLDeliveryStatus


class DHLReturnBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    shipment_id: UUID = Field(alias="shipmentId")


class DHLReturnResponse(BaseModel):
    return_id: UUID = Field(alias="returnId")
    shipment_id: UUID = Field(alias="shipmentId")
    status: DHLReturnStatus


class DHLAdapter:
    """Translate the simulated DHL contract to and from the shared model."""

    return_body_model: type[BaseModel] = DHLReturnBody
    delivery_response_model: type[BaseModel] = DHLDeliveryResponse
    return_response_model: type[BaseModel] = DHLReturnResponse

    def parse_return(self, body: object) -> UUID:
        return DHLReturnBody.model_validate(body).shipment_id

    def serialize_delivery(self, delivery: Delivery) -> DHLDeliveryResponse:
        return DHLDeliveryResponse(
            shipmentId=delivery.id, status=_DELIVERY_STATUSES[delivery.status]
        )

    def serialize_return(self, returned: Return) -> DHLReturnResponse:
        return DHLReturnResponse(
            returnId=returned.id,
            shipmentId=returned.delivery_id,
            status=_RETURN_STATUSES[returned.status],
        )
