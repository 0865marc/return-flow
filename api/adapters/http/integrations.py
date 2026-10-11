from types import UnionType
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends, status
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, JsonValue, ValidationError

from adapters.carriers.contracts import CarrierAdapter
from application.ports.delivery_flow import DeliveryFlow
from composition import CARRIER_ADAPTERS

from .dependencies import get_carrier_adapter, get_delivery_flow, get_delivery_update_flow


def _response_models(models: list[type[BaseModel]]) -> type[BaseModel] | UnionType:
    """Document registered response formats without naming carriers in routes."""
    combined: type[BaseModel] | UnionType = models[0]
    for model in models[1:]:
        combined = combined | model
    return combined


router = APIRouter(prefix="/integrations", tags=["Carriers (simulated contracts)"])
CarrierAdapterDependency = Annotated[CarrierAdapter, Depends(get_carrier_adapter)]
DeliveryFlowDependency = Annotated[DeliveryFlow, Depends(get_delivery_flow)]
DeliveryUpdateFlowDependency = Annotated[DeliveryFlow, Depends(get_delivery_update_flow)]

_delivery_response_models = _response_models(
    [adapter.delivery_response_model for adapter in CARRIER_ADAPTERS.values()]
)
_return_response_models = _response_models(
    [adapter.return_response_model for adapter in CARRIER_ADAPTERS.values()]
)


@router.post(
    "/deliveries",
    status_code=status.HTTP_201_CREATED,
    response_model=None,
    responses={201: {"model": _delivery_response_models}},
)
async def create_delivery(
    adapter: CarrierAdapterDependency, flow: DeliveryFlowDependency
) -> BaseModel:
    """Create a delivery; X-Carrier selects its simulated response format."""
    delivery = await flow.create_delivery()
    return adapter.serialize_delivery(delivery)


@router.post(
    "/deliveries/{delivery_id}/deliver",
    response_model=None,
    responses={200: {"model": _delivery_response_models}},
)
async def mark_delivered(
    delivery_id: UUID,
    adapter: CarrierAdapterDependency,
    flow: DeliveryUpdateFlowDependency,
) -> BaseModel:
    """Confirm delivery through the same input port for every carrier."""
    delivery = await flow.mark_delivered(delivery_id)
    return adapter.serialize_delivery(delivery)


@router.post(
    "/returns",
    status_code=status.HTTP_201_CREATED,
    response_model=None,
    responses={201: {"model": _return_response_models}},
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "schema": {
                        "oneOf": [
                            adapter.return_body_model.model_json_schema()
                            for adapter in CARRIER_ADAPTERS.values()
                        ]
                    }
                }
            }
        }
    },
)
async def request_return(
    body: Annotated[dict[str, JsonValue], Body()],
    adapter: CarrierAdapterDependency,
    flow: DeliveryFlowDependency,
) -> BaseModel:
    """Validate the selected carrier's body, then request a return."""
    try:
        delivery_id = adapter.parse_return(body)
    except ValidationError as error:
        raise RequestValidationError(
            [
                {**detail, "loc": ("body", *detail["loc"])}
                for detail in error.errors()
            ],
            body=body,
        ) from error
    returned = await flow.request_return(delivery_id)
    return adapter.serialize_return(returned)
