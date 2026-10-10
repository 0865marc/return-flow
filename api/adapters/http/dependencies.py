from fastapi import HTTPException, Request, status

from adapters.messaging.contracts import CommandPublisher, CommandStore
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
