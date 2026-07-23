from types import TracebackType

from sqlalchemy.ext.asyncio import AsyncSession

from src.db.repository.outbox import OutboxRepository
from src.db.repository.payments import PaymentRepository


class PaymentsUnitOfWork:
    def __init__(self, session: AsyncSession):
        self._session = session
        self.payments = PaymentRepository(session)
        self.outbox = OutboxRepository(session)

    async def __aenter__(self) -> PaymentsUnitOfWork:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            await self.rollback()

    async def flush(self) -> None:
        await self._session.flush()

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()
