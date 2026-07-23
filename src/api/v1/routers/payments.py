from uuid import UUID

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Header, HTTPException, status

from src.api.v1.schemas.payments import PaymentCreated, PaymentGET, PaymentPOST
from src.application.payments.service import PaymentService
from src.container import Container

payments_router = APIRouter(
    prefix="/payments",
    tags=["payments"],
)


@payments_router.get("/{payment_id}", response_model=PaymentGET)
@inject
async def get_payment(
    payment_id: UUID,
    payment_service: PaymentService = Depends(Provide[Container.payment_service]),
):
    payment = await payment_service.get_payment(payment_id=payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        )

    return {
        "payment_id": payment.payment_id,
        "amount": payment.amount,
        "currency": payment.currency,
        "description": payment.description,
        "metadata": payment.metadata_,
        "status": payment.status,
        "webhook_url": payment.webhook_url,
        "created_at": payment.created_at,
        "processed_at": payment.processed_at,
    }


@payments_router.post("/", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentCreated)
@inject
async def post_payment(
    payment: PaymentPOST,
    payment_service: PaymentService = Depends(Provide[Container.payment_service]),
    idempotency_key: str = Header(min_length=1, max_length=255, alias="Idempotency-Key"),
):
    try:
        new_payment = await payment_service.create_payment(
            data=payment, idempotency_key=idempotency_key
        )
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(error),
        ) from error
    return {
        "payment_id": new_payment.payment_id,
        "status": new_payment.status,
        "created_at": new_payment.created_at,
    }
