from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row

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
            for statement in SCHEMA_STATEMENTS:
                await cursor.execute(statement)


class PostgresDeliveryRepository(DeliveryRepository):
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url

    async def add(self, delivery: Delivery) -> None:
        async with await AsyncConnection.connect(
            self.database_url, connect_timeout=5
        ) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "INSERT INTO deliveries (id, status) VALUES (%s, %s)",
                    (delivery.id, delivery.status.value),
                )

    async def get(self, delivery_id: UUID) -> Delivery | None:
        async with await AsyncConnection.connect(
            self.database_url, connect_timeout=5
        ) as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    "SELECT id, status FROM deliveries WHERE id = %s",
                    (delivery_id,),
                )
                row = await cursor.fetchone()
                return Delivery.model_validate(row) if row is not None else None

    async def save(self, delivery: Delivery, *, expected_status: DeliveryStatus) -> None:
        async with await AsyncConnection.connect(
            self.database_url, connect_timeout=5
        ) as connection:
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


class PostgresReturnRepository(ReturnRepository):
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url

    async def add(self, returned: Return) -> None:
        async with await AsyncConnection.connect(
            self.database_url, connect_timeout=5
        ) as connection:
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
        async with await AsyncConnection.connect(
            self.database_url, connect_timeout=5
        ) as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    "SELECT id, delivery_id, status FROM returns WHERE id = %s",
                    (return_id,),
                )
                row = await cursor.fetchone()
                return Return.model_validate(row) if row is not None else None


@asynccontextmanager
async def open_repositories(database_url: str) -> AsyncGenerator[Repositories]:
    """Initialize PostgreSQL; each repository operation opens its own connection."""
    await initialize_database(database_url)
    yield Repositories(
        deliveries=PostgresDeliveryRepository(database_url),
        returns=PostgresReturnRepository(database_url),
    )
