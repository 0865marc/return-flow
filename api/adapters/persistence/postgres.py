from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from application.errors import ConcurrentModificationError, EntityNotFoundError
from application.ports.repositories import DeliveryRepository, ReturnRepository
from domain.delivery import Delivery, DeliveryStatus
from domain.returns import Return


def initialize_database(database_url: str) -> None:
    """Create the MVP tables without replacing existing data."""
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS deliveries (
                    id UUID PRIMARY KEY,
                    status TEXT NOT NULL CHECK (status IN ('pending', 'delivered'))
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS returns (
                    id UUID PRIMARY KEY,
                    delivery_id UUID NOT NULL REFERENCES deliveries (id),
                    status TEXT NOT NULL CHECK (status IN ('requested', 'completed'))
                )
                """
            )


class PostgresDeliveryRepository(DeliveryRepository):
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url

    def add(self, delivery: Delivery) -> None:
        with psycopg.connect(self.database_url, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO deliveries (id, status) VALUES (%s, %s)",
                    (delivery.id, delivery.status.value),
                )

    def get(self, delivery_id: UUID) -> Delivery | None:
        with psycopg.connect(
            self.database_url, connect_timeout=5, row_factory=dict_row
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id, status FROM deliveries WHERE id = %s",
                    (delivery_id,),
                )
                row = cursor.fetchone()
                return Delivery.model_validate(row) if row is not None else None

    def save(self, delivery: Delivery, *, expected_status: DeliveryStatus) -> None:
        with psycopg.connect(self.database_url, connect_timeout=5) as connection:
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


class PostgresReturnRepository(ReturnRepository):
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url

    def add(self, returned: Return) -> None:
        with psycopg.connect(self.database_url, connect_timeout=5) as connection:
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
        with psycopg.connect(
            self.database_url, connect_timeout=5, row_factory=dict_row
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id, delivery_id, status FROM returns WHERE id = %s",
                    (return_id,),
                )
                row = cursor.fetchone()
                return Return.model_validate(row) if row is not None else None
