import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from psycopg import OperationalError

from adapters.http import register_exception_handlers, router
from adapters.persistence.postgres import (
    PostgresDeliveryRepository,
    PostgresReturnRepository,
    initialize_database,
)


def create_app(database_url: str | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(application: FastAPI):
        url = database_url or os.environ.get("DATABASE_URL")
        if not url:
            raise RuntimeError("DATABASE_URL must be configured to start the API.")
        await run_in_threadpool(initialize_database, url)
        application.state.delivery_repository = PostgresDeliveryRepository(url)
        application.state.return_repository = PostgresReturnRepository(url)
        yield

    application = FastAPI(title="Return-Flow", lifespan=lifespan)
    application.include_router(router)
    register_exception_handlers(application)

    @application.exception_handler(OperationalError)
    async def database_unavailable(
        request: Request, error: OperationalError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503, content={"detail": "Database is unavailable."}
        )

    @application.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return application


app = create_app()
