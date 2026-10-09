from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

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
