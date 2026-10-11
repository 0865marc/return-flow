from uuid import UUID

from adapters.commands.contracts import (
    Command,
    CommandHandler,
    CommandRecord,
    IdempotencyConflictError,
)
from adapters.persistence import Repositories
from application.errors import BusinessRuleViolationError, EntityNotFoundError
from tests.application.fakes import InMemoryDeliveryRepository, InMemoryReturnRepository


class FakeCommandStore:
    """HTTP contract fake; transactional guarantees are tested against PostgreSQL."""

    def __init__(self, repositories: Repositories | None = None):
        self.repositories = repositories or Repositories(
            InMemoryDeliveryRepository(), InMemoryReturnRepository()
        )
        self.commands: dict[UUID, Command] = {}
        self.records: dict[UUID, CommandRecord] = {}

    async def register(self, command: Command) -> CommandRecord:
        previous = self.commands.get(command.command_id)
        if previous is not None and previous != command:
            raise IdempotencyConflictError(
                "Idempotency-Key already identifies a different command."
            )
        if previous is None:
            self.commands[command.command_id] = command
            self.records[command.command_id] = CommandRecord(
                command_id=command.command_id, type=command.type
            )
        return self.records[command.command_id]

    async def get(self, command_id: UUID) -> CommandRecord | None:
        return self.records.get(command_id)

    async def execute(self, command: Command, handler: CommandHandler) -> CommandRecord:
        record = await self.register(command)
        if record.status != "pending":
            return record
        try:
            result = await handler(self.repositories)
        except (EntityNotFoundError, BusinessRuleViolationError) as error:
            record = CommandRecord(
                command_id=command.command_id, type=command.type, status="failed",
                error=str(error), error_code="not_found"
                if isinstance(error, EntityNotFoundError) else "business_rule",
            )
        else:
            record = CommandRecord(
                command_id=command.command_id, type=command.type,
                status="completed", result=result,
            )
        self.records[command.command_id] = record
        return record
