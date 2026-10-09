from fastapi import Request

from application.ports.repositories import DeliveryRepository, ReturnRepository


def get_delivery_repository(request: Request) -> DeliveryRepository:
    return request.app.state.delivery_repository


def get_return_repository(request: Request) -> ReturnRepository:
    return request.app.state.return_repository
