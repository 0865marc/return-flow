from uuid import UUID

from application.errors import BusinessRuleViolationError, EntityNotFoundError
from application.ports.repositories import DeliveryRepository, ReturnRepository
from application.use_cases.deliveries import GetDelivery
from domain import Return


class RequestReturn:
    def __init__(
        self,
        delivery_repository: DeliveryRepository,
        return_repository: ReturnRepository,
    ) -> None:
        self.delivery_repository = delivery_repository
        self.return_repository = return_repository

    async def execute(self, delivery_id: UUID) -> Return:
        delivery = await GetDelivery(self.delivery_repository).execute(delivery_id)
        try:
            returned = Return.request(delivery)
        except ValueError as error:
            raise BusinessRuleViolationError(str(error)) from error
        await self.return_repository.add(returned)
        return returned


class GetReturn:
    def __init__(self, repository: ReturnRepository) -> None:
        self.repository = repository

    async def execute(self, return_id: UUID) -> Return:
        returned = await self.repository.get(return_id)
        if returned is None:
            raise EntityNotFoundError(f"Return {return_id} was not found.")
        return returned
