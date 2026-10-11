import os
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from adapters.messaging.publisher import RabbitMQPublisher
from adapters.messaging.settings import MessagingSettings
from adapters.persistence import Repositories, postgres

PersistenceFactory = Callable[[str], AbstractAsyncContextManager[Repositories]]


@asynccontextmanager
async def open_persistence(
    database_url: str | None = None,
    *,
    factory: PersistenceFactory | None = None,
) -> AsyncGenerator[Repositories]:
    url = database_url or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL must be configured to start the API.")
    persistence = (
        factory(url)
        if factory is not None
        else postgres.open_repositories(url)
    )
    async with persistence as repositories:
        yield repositories


@asynccontextmanager
async def open_publisher() -> AsyncGenerator[RabbitMQPublisher | None]:
    # Synchronous HTTP routes and benchmarks can run without a broker configured.
    if not os.environ.get("RABBITMQ_URL"):
        yield None
        return
    publisher = RabbitMQPublisher(MessagingSettings.from_env())
    try:
        yield publisher
    finally:
        await publisher.close()
