import os
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from adapters.carriers.contracts import CarrierAdapter
from adapters.carriers.correos import CorreosAdapter
from adapters.carriers.dhl import DHLAdapter
from adapters.messaging.publisher import RabbitMQPublisher
from adapters.messaging.settings import MessagingSettings
from adapters.persistence import Repositories, postgres

PersistenceFactory = Callable[[str], AbstractAsyncContextManager[Repositories]]

# Register payload formats here; HTTP routes depend on their common contract.
CARRIER_ADAPTERS: dict[str, CarrierAdapter] = {
    "correos": CorreosAdapter(),
    "dhl": DHLAdapter(),
}


@asynccontextmanager
async def open_persistence(
    database_url: str | None = None,
    *,
    factory: PersistenceFactory | None = None,
    autocommit: bool = False,
) -> AsyncGenerator[Repositories]:
    url = database_url or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL must be configured to start the API.")
    persistence = (
        factory(url)
        if factory is not None
        else postgres.open_repositories(url, autocommit=autocommit)
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
