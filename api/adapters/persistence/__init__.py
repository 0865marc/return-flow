from dataclasses import dataclass

from application.ports.repositories import DeliveryRepository, ReturnRepository


@dataclass(frozen=True)
class Repositories:
    deliveries: DeliveryRepository
    returns: ReturnRepository
