from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Annotated, Literal, Protocol, cast
from uuid import UUID, uuid4

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, TypeAdapter

if TYPE_CHECKING:
    from adapters.persistence import Repositories


class CommandEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: Literal[1] = 1
    command_id: UUID = Field(default_factory=uuid4)


class CreateDeliveryCommand(CommandEnvelope):
    type: Literal["create_delivery"] = "create_delivery"


class EnsureDeliveryDelivered(CommandEnvelope):
    type: Literal["ensure_delivery_delivered"] = "ensure_delivery_delivered"
    delivery_id: UUID


class RequestReturnCommand(CommandEnvelope):
    type: Literal["request_return"] = "request_return"
    delivery_id: UUID


def require_command_id(value: object) -> object:
    # Constructors may generate an ID, but a received message must carry one:
    # generating a fresh ID on each redelivery would bypass deduplication.
    if isinstance(value, dict) and "command_id" not in value:
        raise ValueError("A received command must include command_id.")
    # Payload types are validated by Pydantic after this presence check.
    return cast(object, value)


Command = Annotated[
    CreateDeliveryCommand | EnsureDeliveryDelivered | RequestReturnCommand,
    Field(discriminator="type"),
    BeforeValidator(require_command_id),
]
command_adapter: TypeAdapter[Command] = TypeAdapter[Command](Command)
CommandType = Literal["create_delivery", "ensure_delivery_delivered", "request_return"]


class CommandRecord(BaseModel):
    command_id: UUID
    type: CommandType
    status: Literal["pending", "completed", "failed"] = "pending"
    result: dict[str, str] | None = None
    error: str | None = None


class IdempotencyConflictError(ValueError):
    """A command identifier was reused for another operation or payload."""


CommandHandler = Callable[["Repositories"], Awaitable[dict[str, str]]]


class CommandStore(Protocol):
    async def register(self, command: Command) -> CommandRecord: ...

    async def get(self, command_id: UUID) -> CommandRecord | None: ...

    async def execute(
        self, command: Command, handler: CommandHandler
    ) -> CommandRecord: ...


class CommandPublisher(Protocol):
    async def publish(self, command: Command) -> None: ...
