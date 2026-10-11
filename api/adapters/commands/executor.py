from adapters.commands.contracts import (
    Command,
    CommandRecord,
    CommandStore,
    CreateDeliveryCommand,
    EnsureDeliveryDelivered,
    RequestReturnCommand,
)
from adapters.persistence import Repositories
from application.errors import BusinessRuleViolationError
from application.ports.repositories import DeliveryRepository
from application.use_cases.deliveries import CreateDelivery, GetDelivery, MarkDeliveryDelivered
from application.use_cases.returns import RequestReturn
from domain import Delivery, DeliveryStatus


class CommandExecutor:
    """Run the same transaction and use cases in HTTP requests and workers."""

    def __init__(self, store: CommandStore) -> None:
        self.store = store

    async def execute(self, command: Command) -> CommandRecord:
        return await self.store.execute(
            command, lambda repositories: execute_command(command, repositories)
        )


async def execute_command(command: Command, repositories: Repositories) -> dict[str, str]:
    if isinstance(command, CreateDeliveryCommand):
        result = await CreateDelivery(repositories.deliveries).execute()
    elif isinstance(command, RequestReturnCommand):
        result = await RequestReturn(
            repositories.deliveries, repositories.returns
        ).execute(command.delivery_id)
    else:
        result = await _ensure_delivered(command, repositories.deliveries)
    return result.model_dump(mode="json")


async def _ensure_delivered(
    command: EnsureDeliveryDelivered, repository: DeliveryRepository
) -> Delivery:
    delivery = await GetDelivery(repository).execute(command.delivery_id)
    if delivery.status == DeliveryStatus.DELIVERED:
        return delivery
    try:
        return await MarkDeliveryDelivered(repository).execute(command.delivery_id)
    except BusinessRuleViolationError:
        # A different command may have completed the transition concurrently.
        delivery = await GetDelivery(repository).execute(command.delivery_id)
        if delivery.status != DeliveryStatus.DELIVERED:
            raise
        return delivery
