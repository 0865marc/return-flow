from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, Header, HTTPException, Request, status

from adapters.carriers.contracts import CarrierAdapter
from adapters.commands.contracts import CommandPublisher, CommandStore
from adapters.commands.flow import IdempotentDeliveryFlow
from application.ports.delivery_flow import DeliveryFlow
from application.ports.repositories import DeliveryRepository, ReturnRepository
from composition import CARRIER_ADAPTERS


async def get_delivery_repository(request: Request) -> DeliveryRepository:
    return request.app.state.delivery_repository


async def get_return_repository(request: Request) -> ReturnRepository:
    return request.app.state.return_repository


def get_carrier_adapter(
    carrier: Annotated[
        str,
        Header(
            alias="X-Carrier",
            description="Simulated payload format; this header is not authentication.",
            json_schema_extra={"enum": list(CARRIER_ADAPTERS)},
        ),
    ],
) -> CarrierAdapter:
    adapter = CARRIER_ADAPTERS.get(carrier)
    if adapter is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Unknown carrier {carrier!r}. Choose from: {', '.join(CARRIER_ADAPTERS)}.",
        )
    return adapter


async def get_command_publisher(request: Request) -> CommandPublisher | None:
    return getattr(request.app.state, "command_publisher", None)


async def get_command_store(request: Request) -> CommandStore:
    store = getattr(request.app.state, "command_store", None)
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Command persistence is not configured.",
        )
    return store


IdempotencyKey = Annotated[UUID, Header(alias="Idempotency-Key")]
OptionalIdempotencyKey = Annotated[UUID | None, Header(alias="Idempotency-Key")]


async def get_delivery_flow(
    store: Annotated[CommandStore, Depends(get_command_store)],
    idempotency_key: IdempotencyKey,
) -> DeliveryFlow:
    return IdempotentDeliveryFlow(store, idempotency_key)


async def get_delivery_update_flow(
    store: Annotated[CommandStore, Depends(get_command_store)],
    idempotency_key: OptionalIdempotencyKey = None,
) -> DeliveryFlow:
    return IdempotentDeliveryFlow(store, idempotency_key or uuid4())
