from uuid import UUID

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from psycopg import OperationalError

from adapters.messaging.contracts import IdempotencyConflictError
from adapters.messaging.publisher import MessagingUnavailableError
from application.errors import BusinessRuleViolationError, EntityNotFoundError


class CommandPublicationUnavailableError(MessagingUnavailableError):
    def __init__(self, command_id: UUID) -> None:
        self.command_id = command_id
        super().__init__("Command publication could not be confirmed.")


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(MessagingUnavailableError)
    async def messaging_unavailable(
        request: Request, error: MessagingUnavailableError
    ) -> JSONResponse:
        content = {
            "detail": "Command publication could not be confirmed. "
            "Retry the same request using the same Idempotency-Key."
        }
        headers = {}
        if isinstance(error, CommandPublicationUnavailableError):
            content["command_id"] = str(error.command_id)
            content["detail"] = (
                "Command publication could not be confirmed. Retry the same request "
                f"using Idempotency-Key: {error.command_id}."
            )
            headers["Location"] = f"/commands/{error.command_id}"
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=content,
            headers=headers,
        )

    @app.exception_handler(IdempotencyConflictError)
    async def idempotency_conflict(
        request: Request, error: IdempotencyConflictError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": str(error)},
        )

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
