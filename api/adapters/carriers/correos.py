"""Fictitious Correos payloads, independent of HTTP or messaging."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from domain import Delivery, DeliveryStatus, Return, ReturnStatus

CorreosDeliveryStatus = Literal["pendiente", "entregada"]
CorreosReturnStatus = Literal["solicitada", "completada"]

_DELIVERY_STATUSES: dict[DeliveryStatus, CorreosDeliveryStatus] = {
    DeliveryStatus.PENDING: "pendiente",
    DeliveryStatus.DELIVERED: "entregada",
}
_RETURN_STATUSES: dict[ReturnStatus, CorreosReturnStatus] = {
    ReturnStatus.REQUESTED: "solicitada",
    ReturnStatus.COMPLETED: "completada",
}


class CorreosDeliveryResponse(BaseModel):
    id_entrega: UUID
    estado: CorreosDeliveryStatus


class CorreosReturnBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id_entrega: UUID


class CorreosReturnResponse(BaseModel):
    id_devolucion: UUID
    id_entrega: UUID
    estado: CorreosReturnStatus


class CorreosAdapter:
    """Translate the simulated Correos contract to and from the shared model."""

    return_body_model: type[BaseModel] = CorreosReturnBody
    delivery_response_model: type[BaseModel] = CorreosDeliveryResponse
    return_response_model: type[BaseModel] = CorreosReturnResponse

    def parse_return(self, body: object) -> UUID:
        return CorreosReturnBody.model_validate(body).id_entrega

    def serialize_delivery(self, delivery: Delivery) -> CorreosDeliveryResponse:
        return CorreosDeliveryResponse(
            id_entrega=delivery.id, estado=_DELIVERY_STATUSES[delivery.status]
        )

    def serialize_return(self, returned: Return) -> CorreosReturnResponse:
        return CorreosReturnResponse(
            id_devolucion=returned.id,
            id_entrega=returned.delivery_id,
            estado=_RETURN_STATUSES[returned.status],
        )
