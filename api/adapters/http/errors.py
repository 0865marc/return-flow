from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from psycopg import OperationalError

from application.errors import BusinessRuleViolationError, EntityNotFoundError


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(EntityNotFoundError)
    async def not_found(request: Request, error: EntityNotFoundError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": str(error)},
        )

    @app.exception_handler(BusinessRuleViolationError)
    async def business_rule_violation(
        request: Request, error: BusinessRuleViolationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": str(error)},
        )

    @app.exception_handler(OperationalError)
    async def database_unavailable(
        request: Request, error: OperationalError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": "Database is unavailable."},
        )
