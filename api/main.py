from contextlib import asynccontextmanager

from fastapi import FastAPI

from adapters.http import register_exception_handlers, router
from composition import open_persistence


def create_app(
    database_url: str | None = None, *, persistence_adapter: str | None = None
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(application: FastAPI):
        async with open_persistence(
            database_url, persistence_adapter=persistence_adapter
        ) as repositories:
            application.state.delivery_repository = repositories.deliveries
            application.state.return_repository = repositories.returns
            yield

    application = FastAPI(title="Return-Flow", lifespan=lifespan)
    application.include_router(router)
    register_exception_handlers(application)

    @application.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return application


app = create_app()
