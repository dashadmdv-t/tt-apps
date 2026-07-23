from fastapi import APIRouter

from src.api.v1.routers.payments import payments_router

v1_router = APIRouter(prefix="/v1")
v1_router.include_router(payments_router)
