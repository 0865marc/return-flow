from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Response, status

from adapters.commands.contracts import (
    Command,
    CommandPublisher,
    CommandStore,
    CreateDeliveryCommand,
    EnsureDeliveryDelivered,
    RequestReturnCommand,
)
from adapters.messaging.publisher import MessagingUnavailableError
from application.errors import EntityNotFoundError
from application.ports.delivery_flow import DeliveryFlow
from application.ports.repositories import DeliveryRepository, ReturnRepository
from application.use_cases.deliveries import GetDelivery
from application.use_cases.returns import GetReturn

from .dependencies import (
    IdempotencyKey,
    OptionalIdempotencyKey,
    get_command_publisher,
    get_command_store,
    get_delivery_repository,
    get_delivery_flow,
    get_delivery_update_flow,
    get_return_repository,
)
from .errors import CommandPublicationUnavailableError
from .integrations import router as integration_router
from .schemas import (
    CommandAcceptedResponse,
    CommandResponse,
    DeliveryResponse,
    RequestReturnBody,
    ReturnResponse,
)

router = APIRouter()

DeliveryRepositoryDependency = Annotated[
    DeliveryRepository, Depends(get_delivery_repository)
]
ReturnRepositoryDependency = Annotated[ReturnRepository, Depends(get_return_repository)]
CommandPublisherDependency = Annotated[
    CommandPublisher | None, Depends(get_command_publisher)
]
CommandStoreDependency = Annotated[CommandStore, Depends(get_command_store)]
DeliveryFlowDependency = Annotated[DeliveryFlow, Depends(get_delivery_flow)]
DeliveryUpdateFlowDependency = Annotated[DeliveryFlow, Depends(get_delivery_update_flow)]


async def _enqueue_command(
    command: Command,
    publisher: CommandPublisher | None,
    response: Response,
) -> CommandAcceptedResponse:
    # RabbitMQ owns accepted work until the worker commits its command and result.
    # Retries may publish again; execution deduplicates them in PostgreSQL.
    try:
        if publisher is None:
            raise MessagingUnavailableError("Messaging is not configured.")
        await publisher.publish(command)
    except MessagingUnavailableError as error:
        raise CommandPublicationUnavailableError(command.command_id) from error
    response.headers["Location"] = f"/commands/{command.command_id}"
    return CommandAcceptedResponse(command_id=command.command_id)


@router.post("/deliveries", status_code=status.HTTP_201_CREATED)
async def create_delivery(flow: DeliveryFlowDependency) -> DeliveryResponse:
    delivery = await flow.create_delivery()
    return DeliveryResponse.model_validate(delivery)


@router.post(
    "/deliveries-async",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        503: {"description": "Retry this request using the same Idempotency-Key."},
    },
)
async def enqueue_create_delivery(
    idempotency_key: IdempotencyKey,
    publisher: CommandPublisherDependency,
    response: Response,
) -> CommandAcceptedResponse:
    """Confirm broker acceptance; command persistence and execution run in the worker."""
    return await _enqueue_command(
        CreateDeliveryCommand(command_id=idempotency_key), publisher, response
    )


@router.get(
    "/commands/{command_id}",
    responses={
        404: {
            "description": "No committed command is visible. Accepted work may still "
            "be queued or executing; this does not prove it was rejected or lost."
        },
    },
)
async def get_command(
    command_id: UUID, store: CommandStoreDependency
) -> CommandResponse:
    """Read persisted state; new queued commands become visible after worker commit."""
    record = await store.get(command_id)
    if record is None:
        raise EntityNotFoundError(f"Command {command_id} was not found.")
    return CommandResponse.model_validate(record)


@router.get("/deliveries/{delivery_id}")
async def get_delivery(
    delivery_id: UUID, repository: DeliveryRepositoryDependency
) -> DeliveryResponse:
    delivery = await GetDelivery(repository).execute(delivery_id)
    return DeliveryResponse.model_validate(delivery)


@router.post("/deliveries/{delivery_id}/deliver")
async def mark_delivery_delivered(
    delivery_id: UUID, flow: DeliveryUpdateFlowDependency
) -> DeliveryResponse:
    delivery = await flow.mark_delivered(delivery_id)
    return DeliveryResponse.model_validate(delivery)


@router.post(
    "/deliveries/{delivery_id}/deliver-async",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        503: {
            "description": "Retry this request using the returned command_id as Idempotency-Key."
        },
    },
)
async def enqueue_delivery(
    delivery_id: UUID,
    publisher: CommandPublisherDependency,
    response: Response,
    idempotency_key: OptionalIdempotencyKey = None,
) -> CommandAcceptedResponse:
    """Accept work for later processing; existence and business rules run in the worker."""
    command = EnsureDeliveryDelivered(
        delivery_id=delivery_id, command_id=idempotency_key or uuid4()
    )
    return await _enqueue_command(command, publisher, response)


@router.post("/returns", status_code=status.HTTP_201_CREATED)
async def request_return(
    body: RequestReturnBody,
    flow: DeliveryFlowDependency,
) -> ReturnResponse:
    returned = await flow.request_return(body.delivery_id)
    return ReturnResponse.model_validate(returned)


@router.post(
    "/returns-async",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        503: {"description": "Retry this request using the same Idempotency-Key."},
    },
)
async def enqueue_request_return(
    body: RequestReturnBody,
    idempotency_key: IdempotencyKey,
    publisher: CommandPublisherDependency,
    response: Response,
) -> CommandAcceptedResponse:
    return await _enqueue_command(
        RequestReturnCommand(command_id=idempotency_key, delivery_id=body.delivery_id),
        publisher,
        response,
    )


@router.get("/returns/{return_id}")
async def get_return(
    return_id: UUID, repository: ReturnRepositoryDependency
) -> ReturnResponse:
    returned = await GetReturn(repository).execute(return_id)
    return ReturnResponse.model_validate(returned)


router.include_router(integration_router)
