from dataclasses import dataclass
from typing import TYPE_CHECKING

from application.ports.repositories import DeliveryRepository, ReturnRepository

if TYPE_CHECKING:
    from adapters.messaging.contracts import CommandStore


@dataclass(frozen=True)
class Repositories:
    deliveries: DeliveryRepository
    returns: ReturnRepository
    commands: "CommandStore | None" = None
