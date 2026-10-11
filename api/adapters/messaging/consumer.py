import logging

from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError

from adapters.commands.contracts import (
    CommandStore,
    IdempotencyConflictError,
    command_adapter,
)
from adapters.commands.executor import CommandExecutor

logger = logging.getLogger(__name__)


class CommandConsumer:
    def __init__(self, store: CommandStore) -> None:
        self.executor = CommandExecutor(store)

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
            record = await self.executor.execute(command)
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
