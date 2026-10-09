import os
from collections.abc import Generator
from contextlib import contextmanager
from math import isfinite
from uuid import UUID

from psycopg import Connection
from psycopg.rows import TupleRow, dict_row
from psycopg_pool import ConnectionPool

from adapters.persistence import Repositories
from adapters.persistence.postgres import initialize_database
from application.errors import ConcurrentModificationError, EntityNotFoundError
from application.ports.repositories import DeliveryRepository, ReturnRepository
from domain.delivery import Delivery, DeliveryStatus
from domain.returns import Return


class PooledPostgresDeliveryRepository(DeliveryRepository):
    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    def add(self, delivery: Delivery) -> None:
        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO deliveries (id, status) VALUES (%s, %s)",
                    (delivery.id, delivery.status.value),
                )

    def get(self, delivery_id: UUID) -> Delivery | None:
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    "SELECT id, status FROM deliveries WHERE id = %s",
                    (delivery_id,),
                )
                row = cursor.fetchone()
                return Delivery.model_validate(row) if row is not None else None

    def save(self, delivery: Delivery, *, expected_status: DeliveryStatus) -> None:
        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE deliveries SET status = %s WHERE id = %s AND status = %s",
                    (delivery.status.value, delivery.id, expected_status.value),
                )
                if cursor.rowcount == 0:
                    cursor.execute(
                        "SELECT 1 FROM deliveries WHERE id = %s", (delivery.id,)
                    )
                    if cursor.fetchone() is None:
                        raise EntityNotFoundError(f"Delivery {delivery.id} not found.")
                    raise ConcurrentModificationError(
                        f"Delivery {delivery.id} was modified by another operation."
                    )


class PooledPostgresReturnRepository(ReturnRepository):
    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    def add(self, returned: Return) -> None:
        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO returns (id, delivery_id, status) VALUES (%s, %s, %s)",
                    (
                        returned.id,
                        returned.delivery_id,
                        returned.status.value,
                    ),
                )

    def get(self, return_id: UUID) -> Return | None:
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    "SELECT id, delivery_id, status FROM returns WHERE id = %s",
                    (return_id,),
                )
                row = cursor.fetchone()
                return Return.model_validate(row) if row is not None else None


@contextmanager
def open_repositories(database_url: str) -> Generator[Repositories]:
    """Own the shared connection pool until the application stops."""
    max_size = int(os.environ.get("DATABASE_POOL_MAX_SIZE", "10"))
    timeout = float(os.environ.get("DATABASE_POOL_TIMEOUT", "5"))
    if max_size < 1:
        raise ValueError("DATABASE_POOL_MAX_SIZE must be at least 1.")
    if not isfinite(timeout) or timeout <= 0:
        raise ValueError("DATABASE_POOL_TIMEOUT must be a positive finite number.")

    pool = ConnectionPool(
        database_url,
        connection_class=Connection[TupleRow],
        min_size=1,
        max_size=max_size,
        timeout=timeout,
        kwargs={"connect_timeout": 5},
        open=False,
    )
    try:
        initialize_database(database_url)
        pool.open(wait=True, timeout=5)
        yield Repositories(
            deliveries=PooledPostgresDeliveryRepository(pool),
            returns=PooledPostgresReturnRepository(pool),
        )
    finally:
        pool.close()
