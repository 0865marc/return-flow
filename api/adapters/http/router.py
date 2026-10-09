from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, status

from application.ports.repositories import DeliveryRepository, ReturnRepository
from application.use_cases.deliveries import (
    CreateDelivery,
    GetDelivery,
    MarkDeliveryDelivered,
)
from application.use_cases.returns import GetReturn, RequestReturn

from .dependencies import get_delivery_repository, get_return_repository
from .schemas import DeliveryResponse, RequestReturnBody, ReturnResponse

router = APIRouter()

DeliveryRepositoryDependency = Annotated[
    DeliveryRepository, Depends(get_delivery_repository)
]
ReturnRepositoryDependency = Annotated[ReturnRepository, Depends(get_return_repository)]


@router.post("/deliveries", status_code=status.HTTP_201_CREATED)
async def create_delivery(repository: DeliveryRepositoryDependency) -> DeliveryResponse:
    delivery = await CreateDelivery(repository).execute()
    return DeliveryResponse.model_validate(delivery)


@router.get("/deliveries/{delivery_id}")
async def get_delivery(
    delivery_id: UUID, repository: DeliveryRepositoryDependency
) -> DeliveryResponse:
    delivery = await GetDelivery(repository).execute(delivery_id)
    return DeliveryResponse.model_validate(delivery)


@router.post("/deliveries/{delivery_id}/deliver")
async def mark_delivery_delivered(
    delivery_id: UUID, repository: DeliveryRepositoryDependency
) -> DeliveryResponse:
    delivery = await MarkDeliveryDelivered(repository).execute(delivery_id)
    return DeliveryResponse.model_validate(delivery)


@router.post("/returns", status_code=status.HTTP_201_CREATED)
async def request_return(
    body: RequestReturnBody,
    delivery_repository: DeliveryRepositoryDependency,
    return_repository: ReturnRepositoryDependency,
) -> ReturnResponse:
    returned = await RequestReturn(delivery_repository, return_repository).execute(
        body.delivery_id
    )
    return ReturnResponse.model_validate(returned)


@router.get("/returns/{return_id}")
async def get_return(
    return_id: UUID, repository: ReturnRepositoryDependency
) -> ReturnResponse:
    returned = await GetReturn(repository).execute(return_id)
    return ReturnResponse.model_validate(returned)
