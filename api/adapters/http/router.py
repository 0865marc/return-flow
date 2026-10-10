from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Header, Response, status

from adapters.messaging.contracts import (
    Command,
    CommandPublisher,
    CommandStore,
    CreateDeliveryCommand,
    EnsureDeliveryDelivered,
    RequestReturnCommand,
)
from adapters.messaging.publisher import MessagingUnavailableError
from application.errors import EntityNotFoundError
from application.ports.repositories import DeliveryRepository, ReturnRepository
from application.use_cases.deliveries import (
    CreateDelivery,
    GetDelivery,
    MarkDeliveryDelivered,
)
from application.use_cases.returns import GetReturn, RequestReturn

from .dependencies import (
    get_command_publisher,
    get_command_store,
    get_delivery_repository,
    get_return_repository,
)
from .errors import CommandPublicationUnavailableError
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
IdempotencyKey = Annotated[UUID, Header(alias="Idempotency-Key")]
OptionalIdempotencyKey = Annotated[UUID | None, Header(alias="Idempotency-Key")]


async def _enqueue_command(
    command: Command,
    store: CommandStore,
    publisher: CommandPublisher | None,
    response: Response,
) -> CommandAcceptedResponse:
    record = await store.register(command)
    if record.status == "pending":
        try:
            if publisher is None:
                raise MessagingUnavailableError("Messaging is not configured.")
            await publisher.publish(command)
        except MessagingUnavailableError as error:
            raise CommandPublicationUnavailableError(command.command_id) from error
    response.headers["Location"] = f"/commands/{command.command_id}"
    return CommandAcceptedResponse(command_id=command.command_id)


@router.post("/deliveries", status_code=status.HTTP_201_CREATED)
async def create_delivery(repository: DeliveryRepositoryDependency) -> DeliveryResponse:
    delivery = await CreateDelivery(repository).execute()
    return DeliveryResponse.model_validate(delivery)


@router.post(
    "/deliveries-async",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        409: {"description": "Idempotency-Key already identifies another command."},
        503: {"description": "Retry this request using the same Idempotency-Key."},
    },
)
async def enqueue_create_delivery(
    idempotency_key: IdempotencyKey,
    store: CommandStoreDependency,
    publisher: CommandPublisherDependency,
    response: Response,
) -> CommandAcceptedResponse:
    return await _enqueue_command(
        CreateDeliveryCommand(command_id=idempotency_key), store, publisher, response
    )


@router.get("/commands/{command_id}")
async def get_command(
    command_id: UUID, store: CommandStoreDependency
) -> CommandResponse:
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
    delivery_id: UUID, repository: DeliveryRepositoryDependency
) -> DeliveryResponse:
    delivery = await MarkDeliveryDelivered(repository).execute(delivery_id)
    return DeliveryResponse.model_validate(delivery)


@router.post(
    "/deliveries/{delivery_id}/deliver-async",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        409: {"description": "Idempotency-Key already identifies another command."},
        503: {
            "description": "Retry this request using the returned command_id as Idempotency-Key."
        },
    },
)
async def enqueue_delivery(
    delivery_id: UUID,
    store: CommandStoreDependency,
    publisher: CommandPublisherDependency,
    response: Response,
    idempotency_key: OptionalIdempotencyKey = None,
) -> CommandAcceptedResponse:
    """Accept work for later processing; existence and business rules run in the worker."""
    command = EnsureDeliveryDelivered(
        delivery_id=delivery_id, command_id=idempotency_key or uuid4()
    )
    return await _enqueue_command(command, store, publisher, response)


@router.post("/returns", status_code=status.HTTP_201_CREATED)
async def request_return(
    body: RequestReturnBody,
    delivery_repository: DeliveryRepositoryDependency,
    return_repository: ReturnRepositoryDependency,
) -> ReturnResponse:
    returned = await RequestReturn(delivery_repository, return_repository).execute(
        body.delivery_id
    )
    return ReturnResponse.model_validate(returned)


@router.post(
    "/returns-async",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        409: {"description": "Idempotency-Key already identifies another command."},
        503: {"description": "Retry this request using the same Idempotency-Key."},
    },
)
async def enqueue_request_return(
    body: RequestReturnBody,
    idempotency_key: IdempotencyKey,
    store: CommandStoreDependency,
    publisher: CommandPublisherDependency,
    response: Response,
) -> CommandAcceptedResponse:
    return await _enqueue_command(
        RequestReturnCommand(command_id=idempotency_key, delivery_id=body.delivery_id),
        store,
        publisher,
        response,
    )


@router.get("/returns/{return_id}")
async def get_return(
    return_id: UUID, repository: ReturnRepositoryDependency
) -> ReturnResponse:
    returned = await GetReturn(repository).execute(return_id)
    return ReturnResponse.model_validate(returned)
