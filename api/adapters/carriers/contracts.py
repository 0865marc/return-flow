from typing import Protocol
from uuid import UUID

from pydantic import BaseModel

from domain import Delivery, Return


class CarrierAdapter(Protocol):
    """Carrier payload translation; the application input port is DeliveryFlow."""

    return_body_model: type[BaseModel]
    delivery_response_model: type[BaseModel]
    return_response_model: type[BaseModel]

    def parse_return(self, body: object) -> UUID: ...

    def serialize_delivery(self, delivery: Delivery) -> BaseModel: ...

    def serialize_return(self, returned: Return) -> BaseModel: ...
