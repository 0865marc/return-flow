from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, Header, HTTPException, Request, status

from adapters.commands.contracts import CommandPublisher, CommandStore
from adapters.commands.flow import IdempotentDeliveryFlow
from application.ports.delivery_flow import DeliveryFlow
from application.ports.repositories import DeliveryRepository, ReturnRepository


async def get_delivery_repository(request: Request) -> DeliveryRepository:
    return request.app.state.delivery_repository


async def get_return_repository(request: Request) -> ReturnRepository:
    return request.app.state.return_repository


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
