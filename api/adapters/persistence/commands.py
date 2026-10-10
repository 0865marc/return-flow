import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from uuid import UUID

from psycopg import AsyncConnection, InterfaceError, OperationalError
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from adapters.messaging.contracts import (
    Command,
    CommandHandler,
    CommandRecord,
    IdempotencyConflictError,
)
from adapters.persistence import Repositories
from adapters.persistence.postgres import (
    PostgresDeliveryRepository,
    PostgresReturnRepository,
)
from application.errors import BusinessRuleViolationError, EntityNotFoundError

logger = logging.getLogger(__name__)

ConnectionFactory = Callable[[], AbstractAsyncContextManager[AsyncConnection]]


class PostgresCommandStore:
    """Serialize each command and commit its business writes with its outcome."""

    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self.connection_factory = connection_factory

    async def register(self, command: Command) -> CommandRecord:
        async with self.connection_factory() as connection:
            async with connection.transaction():
                await self._insert_pending(connection, command)
                record = await self._load(connection, command)
        return record

    async def get(self, command_id: UUID) -> CommandRecord | None:
        async with self.connection_factory() as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    "SELECT command_id, type, status, result, error "
                    "FROM commands WHERE command_id = %s",
                    (command_id,),
                )
                row = await cursor.fetchone()
        return CommandRecord.model_validate(row) if row is not None else None

    async def execute(self, command: Command, handler: CommandHandler) -> CommandRecord:
        async with self.connection_factory() as connection:
            async with connection.transaction():
                # Also accepts messages published before API-side registration existed.
                await self._insert_pending(connection, command)
                record = await self._load(connection, command, lock=True)
                if record.status == "pending":
                    repositories = Repositories(
                        deliveries=PostgresDeliveryRepository(
                            "", connection=connection
                        ),
                        returns=PostgresReturnRepository("", connection=connection),
                    )
                    try:
                        # A savepoint rolls back partial business writes on failure,
                        # while the outer transaction persists the terminal outcome.
                        async with connection.transaction():
                            result = await handler(repositories)
                    except (OperationalError, InterfaceError):
                        # Connection failures must be retried by message redelivery.
                        raise
                    except Exception as error:
                        if isinstance(
                            error, (EntityNotFoundError, BusinessRuleViolationError)
                        ):
                            message = str(error)
                        else:
                            logger.exception("Command %s failed", command.command_id)
                            message = "Command execution failed."
                        record = CommandRecord(
                            command_id=command.command_id,
                            type=command.type,
                            status="failed",
                            error=message,
                        )
                    else:
                        record = CommandRecord(
                            command_id=command.command_id,
                            type=command.type,
                            status="completed",
                            result=result,
                        )
                    await connection.execute(
                        "UPDATE commands SET status = %s, result = %s, error = %s "
                        "WHERE command_id = %s",
                        (
                            record.status,
                            Jsonb(record.result) if record.result is not None else None,
                            record.error,
                            command.command_id,
                        ),
                    )
        # Both context managers have finished: COMMIT errors propagate before
        # the worker can receive a terminal record and acknowledge the message.
        return record

    @staticmethod
    async def _insert_pending(connection: AsyncConnection, command: Command) -> None:
        await connection.execute(
            "INSERT INTO commands (command_id, type, payload, status) "
            "VALUES (%s, %s, %s, 'pending') "
            "ON CONFLICT (command_id) DO NOTHING",
            (command.command_id, command.type, Jsonb(command.model_dump(mode="json"))),
        )

    @staticmethod
    async def _load(
        connection: AsyncConnection, command: Command, *, lock: bool = False
    ) -> CommandRecord:
        query = (
            "SELECT command_id, type, payload, status, result, error "
            "FROM commands WHERE command_id = %s"
        )
        if lock:
            query += " FOR UPDATE"
        async with connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(query, (command.command_id,))
            row = await cursor.fetchone()
        if row is None:
            raise RuntimeError("Registered command was not found.")
        if row.pop("payload") != command.model_dump(mode="json"):
            raise IdempotencyConflictError(
                "This idempotency key was already used for a different command."
            )
        return CommandRecord.model_validate(row)
