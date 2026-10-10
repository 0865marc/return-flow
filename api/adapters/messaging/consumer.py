import logging

from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError

from adapters.messaging.contracts import (
    Command,
    CommandStore,
    CreateDeliveryCommand,
    EnsureDeliveryDelivered,
    IdempotencyConflictError,
    RequestReturnCommand,
    command_adapter,
)
from adapters.persistence import Repositories
from application.errors import BusinessRuleViolationError
from application.ports.repositories import DeliveryRepository
from application.use_cases.deliveries import CreateDelivery, GetDelivery, MarkDeliveryDelivered
from application.use_cases.returns import RequestReturn
from domain import Delivery, DeliveryStatus

logger = logging.getLogger(__name__)


class CommandConsumer:
    def __init__(self, store: CommandStore) -> None:
        self.store = store

    async def handle(self, message: AbstractIncomingMessage) -> None:
        command_id = message.message_id or "unknown"
        try:
            command = command_adapter.validate_json(message.body)
        except ValidationError:
            logger.exception("Invalid command command_id=%s", command_id)
            await message.reject(requeue=False)
            return

        command_id = str(command.command_id)
        try:
            record = await self.store.execute(
                command, lambda repositories: self._execute(command, repositories)
            )
        except IdempotencyConflictError:
            logger.exception("Conflicting command command_id=%s", command_id)
            await message.reject(requeue=False)
            return

        if record.status == "failed":
            logger.warning("Command failed command_id=%s error=%s", command_id, record.error)
            await message.reject(requeue=False)
            return
        if record.status != "completed":
            raise RuntimeError("Command execution did not reach a terminal state.")

        # execute returns only after COMMIT. Database/connection errors and
        # cancellation propagate: closing the worker leaves the message available
        # for redelivery. An ACK failure also must not dead-letter committed work.
        await message.ack()
        logger.info("Command completed command_id=%s", command_id)

    async def _execute(self, command: Command, repositories: Repositories) -> dict[str, str]:
        if isinstance(command, CreateDeliveryCommand):
            result = await CreateDelivery(repositories.deliveries).execute()
        elif isinstance(command, RequestReturnCommand):
            result = await RequestReturn(
                repositories.deliveries, repositories.returns
            ).execute(command.delivery_id)
        else:
            result = await self._ensure_delivered(command, repositories.deliveries)
        return result.model_dump(mode="json")

    async def _ensure_delivered(
        self, command: EnsureDeliveryDelivered, repository: DeliveryRepository
    ) -> Delivery:
        delivery = await GetDelivery(repository).execute(command.delivery_id)
        if delivery.status == DeliveryStatus.DELIVERED:
            return delivery

        try:
            return await MarkDeliveryDelivered(repository).execute(command.delivery_id)
        except BusinessRuleViolationError:
            # Another consumer or the synchronous HTTP endpoint may complete
            # the transition between either read and the conditional write.
            delivery = await GetDelivery(repository).execute(command.delivery_id)
            if delivery.status != DeliveryStatus.DELIVERED:
                raise
            return delivery
