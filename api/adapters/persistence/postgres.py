import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from math import isfinite
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row
from psycopg_pool import AsyncConnectionPool

from adapters.persistence import Repositories
from adapters.persistence.schema import SCHEMA_STATEMENTS
from application.errors import ConcurrentModificationError, EntityNotFoundError
from application.ports.repositories import DeliveryRepository, ReturnRepository
from domain.delivery import Delivery, DeliveryStatus
from domain.returns import Return


async def initialize_database(database_url: str) -> None:
    """Create the MVP tables without replacing existing data."""
    async with await AsyncConnection.connect(
        database_url, connect_timeout=5
    ) as connection:
        async with connection.cursor() as cursor:
            # API and workers can bootstrap the same schema concurrently. IF NOT
            # EXISTS alone does not prevent races in PostgreSQL's system catalog.
            await cursor.execute(
                "SELECT pg_advisory_xact_lock("
                "hashtext('return_flow.schema'), hashtext(current_schema()))"
            )
            for statement in SCHEMA_STATEMENTS:
                await cursor.execute(statement)


class _PostgresRepository:
    def __init__(
        self,
        pool: AsyncConnectionPool[AsyncConnection[TupleRow]] | None = None,
        *,
        connection: AsyncConnection | None = None,
    ) -> None:
        if (pool is None) == (connection is None):
            raise ValueError("Provide either a connection pool or a bound connection.")
        self.pool = pool
        self.connection = connection

    @asynccontextmanager
    async def _connection(self) -> AsyncGenerator[AsyncConnection]:
        if self.connection is not None:
            # The command store owns this connection and its transaction.
            yield self.connection
        else:
            assert self.pool is not None
            async with self.pool.connection() as connection:
                yield connection


class PostgresDeliveryRepository(_PostgresRepository, DeliveryRepository):
    async def add(self, delivery: Delivery) -> None:
        async with self._connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "INSERT INTO deliveries (id, status) VALUES (%s, %s)",
                    (delivery.id, delivery.status.value),
                )

    async def get(self, delivery_id: UUID) -> Delivery | None:
        async with self._connection() as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    "SELECT id, status FROM deliveries WHERE id = %s",
                    (delivery_id,),
                )
                row = await cursor.fetchone()
                return Delivery.model_validate(row) if row is not None else None

    async def save(self, delivery: Delivery, *, expected_status: DeliveryStatus) -> None:
        async with self._connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "UPDATE deliveries SET status = %s WHERE id = %s AND status = %s",
                    (delivery.status.value, delivery.id, expected_status.value),
                )
                if cursor.rowcount == 0:
                    await cursor.execute(
                        "SELECT 1 FROM deliveries WHERE id = %s", (delivery.id,)
                    )
                    if await cursor.fetchone() is None:
                        raise EntityNotFoundError(f"Delivery {delivery.id} not found.")
                    raise ConcurrentModificationError(
                        f"Delivery {delivery.id} was modified by another operation."
                    )


class PostgresReturnRepository(_PostgresRepository, ReturnRepository):
    async def add(self, returned: Return) -> None:
        async with self._connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "INSERT INTO returns (id, delivery_id, status) VALUES (%s, %s, %s)",
                    (
                        returned.id,
                        returned.delivery_id,
                        returned.status.value,
                    ),
                )

    async def get(self, return_id: UUID) -> Return | None:
        async with self._connection() as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    "SELECT id, delivery_id, status FROM returns WHERE id = %s",
                    (return_id,),
                )
                row = await cursor.fetchone()
                return Return.model_validate(row) if row is not None else None


@asynccontextmanager
async def open_repositories(
    database_url: str, *, autocommit: bool = False
) -> AsyncGenerator[Repositories]:
    """Own one shared asynchronous pool until the application stops."""
    from adapters.persistence.commands import PostgresCommandStore

    max_size = int(os.environ.get("DATABASE_POOL_MAX_SIZE", "10"))
    timeout = float(os.environ.get("DATABASE_POOL_TIMEOUT", "5"))
    if max_size < 1:
        raise ValueError("DATABASE_POOL_MAX_SIZE must be at least 1.")
    if not isfinite(timeout) or timeout <= 0:
        raise ValueError("DATABASE_POOL_TIMEOUT must be a positive finite number.")

    pool = AsyncConnectionPool(
        database_url,
        connection_class=AsyncConnection[TupleRow],
        min_size=1,
        max_size=max_size,
        timeout=timeout,
        # Command writes use explicit transactions in PostgresCommandStore.
        # Autocommit lets API reads release the connection without BEGIN/COMMIT.
        kwargs={"connect_timeout": 5, "autocommit": autocommit},
        open=False,
    )
    try:
        await initialize_database(database_url)
        await pool.open(wait=True, timeout=5)
        yield Repositories(
            deliveries=PostgresDeliveryRepository(pool),
            returns=PostgresReturnRepository(pool),
            commands=PostgresCommandStore(pool.connection),
        )
    finally:
        await pool.close()
