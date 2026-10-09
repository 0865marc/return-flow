import os
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractContextManager, asynccontextmanager

from fastapi.concurrency import contextmanager_in_threadpool

from adapters.persistence import Repositories, postgres

PERSISTENCE_FACTORIES: dict[
    str, Callable[[str], AbstractContextManager[Repositories]]
] = {
    "postgres": postgres.open_repositories,
}


@asynccontextmanager
async def open_persistence(
    database_url: str | None = None, *, persistence_adapter: str | None = None
) -> AsyncGenerator[Repositories]:
    """Select persistence and manage its lifetime outside the async event loop."""
    url = database_url or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL must be configured to start the API.")
    adapter = (
        persistence_adapter
        if persistence_adapter is not None
        else os.environ.get("PERSISTENCE_ADAPTER", "postgres")
    )
    factory = PERSISTENCE_FACTORIES.get(adapter)
    if factory is None:
        choices = ", ".join(PERSISTENCE_FACTORIES)
        raise ValueError(f"Unknown persistence adapter {adapter!r}. Choose from: {choices}.")

    async with contextmanager_in_threadpool(factory(url)) as repositories:
        yield repositories
