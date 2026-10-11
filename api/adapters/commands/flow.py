from uuid import UUID

from adapters.commands.contracts import (
    Command,
    CommandExecutionFailedError,
    CommandStore,
    CreateDeliveryCommand,
    EnsureDeliveryDelivered,
    RequestReturnCommand,
)
from adapters.commands.executor import CommandExecutor
from application.errors import BusinessRuleViolationError, EntityNotFoundError
from domain import Delivery, Return


class IdempotentDeliveryFlow:
    """Implement DeliveryFlow with one command identity per incoming request."""

    def __init__(self, store: CommandStore, command_id: UUID) -> None:
        self.executor = CommandExecutor(store)
        self.command_id = command_id

    async def create_delivery(self) -> Delivery:
        result = await self._result(CreateDeliveryCommand(command_id=self.command_id))
        return Delivery.model_validate(result)

    async def mark_delivered(self, delivery_id: UUID) -> Delivery:
        result = await self._result(
            EnsureDeliveryDelivered(command_id=self.command_id, delivery_id=delivery_id)
        )
        return Delivery.model_validate(result)

    async def request_return(self, delivery_id: UUID) -> Return:
        result = await self._result(
            RequestReturnCommand(command_id=self.command_id, delivery_id=delivery_id)
        )
        return Return.model_validate(result)

    async def _result(self, command: Command) -> dict[str, str]:
        record = await self.executor.execute(command)
        if record.status == "failed":
            if record.error_code == "not_found":
                raise EntityNotFoundError(record.error)
            if record.error_code == "business_rule":
                raise BusinessRuleViolationError(record.error)
            raise CommandExecutionFailedError(record.error or "Command execution failed.")
        if record.status != "completed" or record.result is None:
            raise RuntimeError("Command execution did not reach a terminal state.")
        return record.result
