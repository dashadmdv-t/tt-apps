from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.payments import Payment


class PaymentRepository:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        return await self._session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        return await self._session.scalar(select(Payment).where(Payment.payment_id == payment_id))

    async def get_by_id_for_update(self, payment_id: UUID) -> Payment | None:
        return await self._session.scalar(
            select(Payment).where(Payment.payment_id == payment_id).with_for_update()
        )

    def add(self, payment: Payment) -> None:
        self._session.add(payment)

    async def refresh(self, payment: Payment) -> None:
        await self._session.refresh(payment)
