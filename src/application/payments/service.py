from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.api.v1.schemas.payments import PaymentPOST
from src.core.enums import OutboxStatus, PaymentStatus
from src.db.uow.payments import PaymentsUnitOfWork
from src.models.outbox import Outbox
from src.models.payments import Payment

PAYMENTS_NEW_TOPIC = "payments.new"


def _same_payload(existing: Payment, data: PaymentPOST) -> bool:
    return (
        existing.amount == data.amount
        and existing.currency == data.currency
        and existing.description == data.description
        and existing.metadata_ == data.metadata
        and existing.webhook_url == (str(data.webhook_url) if data.webhook_url else None)
    )


class PaymentService:
    def __init__(self, session_factory: async_sessionmaker):
        self._session_factory = session_factory

    async def create_payment(self, data: PaymentPOST, idempotency_key: str) -> Payment:
        async with self._session_factory() as session:
            async with PaymentsUnitOfWork(session) as uow:
                existing = await uow.payments.get_by_idempotency_key(
                    idempotency_key=idempotency_key
                )
                if existing is not None:
                    if not _same_payload(existing, data):
                        raise ValueError("Idempotency key already used with different payload")
                    return existing

                payment = Payment(
                    amount=data.amount,
                    currency=data.currency,
                    description=data.description,
                    metadata_=data.metadata,
                    status=PaymentStatus.PENDING,
                    idempotency_key=idempotency_key,
                    webhook_url=str(data.webhook_url) if data.webhook_url else None,
                )
                uow.payments.add(payment)

                try:
                    await session.flush()
                except IntegrityError as error:
                    await session.rollback()
                    existing = await uow.payments.get_by_idempotency_key(
                        idempotency_key=idempotency_key
                    )
                    if existing is None:
                        raise
                    if not _same_payload(existing, data):
                        raise ValueError(
                            "Idempotency key already used with different payload"
                        ) from error
                    return existing

                uow.outbox.add(
                    Outbox(
                        topic=PAYMENTS_NEW_TOPIC,
                        payload={
                            "payment_id": str(payment.payment_id),
                            "idempotency_key": idempotency_key,
                        },
                        status=OutboxStatus.PENDING,
                    )
                )
                await session.commit()
                await session.refresh(payment)
                return payment

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        async with self._session_factory() as session:
            async with PaymentsUnitOfWork(session) as uow:
                return await uow.payments.get_by_id(payment_id)
