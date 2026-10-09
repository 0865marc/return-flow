import os
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from adapters.persistence import Repositories, postgres, postgres_pool

PersistenceFactory = Callable[[str], AbstractAsyncContextManager[Repositories]]

PERSISTENCE_FACTORIES: dict[str, PersistenceFactory] = {
    "postgres": postgres.open_repositories,
    "postgres_pool": postgres_pool.open_repositories,
}


def get_persistence_factory(persistence_adapter: str | None = None) -> PersistenceFactory:
    adapter = (
        persistence_adapter
        if persistence_adapter is not None
        else os.environ.get("PERSISTENCE_ADAPTER", "postgres_pool")
    )
    factory = PERSISTENCE_FACTORIES.get(adapter)
    if factory is None:
        choices = ", ".join(PERSISTENCE_FACTORIES)
        raise ValueError(f"Unknown persistence adapter {adapter!r}. Choose from: {choices}.")
    return factory


@asynccontextmanager
async def open_persistence(
    database_url: str | None = None, *, factory: PersistenceFactory
) -> AsyncGenerator[Repositories]:
    url = database_url or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL must be configured to start the API.")
    async with factory(url) as repositories:
        yield repositories
