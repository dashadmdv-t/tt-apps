from datetime import datetime

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.enums import OutboxStatus
from src.models.outbox import Outbox


class OutboxRepository:
    def __init__(self, session: AsyncSession):
        self._session = session

    def add(self, outbox: Outbox) -> None:
        self._session.add(outbox)

    async def get_ready_for_dispatch(self, *, now: datetime, limit: int) -> list[Outbox]:
        query = (
            select(Outbox)
            .where(
                or_(
                    Outbox.status == OutboxStatus.PENDING,
                    and_(
                        Outbox.status == OutboxStatus.PROCESSING,
                        Outbox.locked_until <= now,
                    ),
                )
            )
            .where(
                or_(
                    Outbox.next_retry_at.is_(None),
                    Outbox.next_retry_at <= now,
                )
            )
            .order_by(Outbox.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.scalars(query)
        return list(result)

    def claim_for_dispatch(self, message: Outbox, *, locked_until: datetime) -> None:
        message.status = OutboxStatus.PROCESSING
        message.locked_until = locked_until

    def mark_published(self, message: Outbox, *, now: datetime) -> None:
        message.status = OutboxStatus.PUBLISHED
        message.published_at = now
        message.locked_until = None
        message.next_retry_at = None
        message.last_error = None

    def schedule_retry(
        self,
        message: Outbox,
        *,
        attempts: int,
        next_retry_at: datetime,
        error: str,
    ) -> None:
        message.attempts = attempts
        message.status = OutboxStatus.PENDING
        message.locked_until = None
        message.next_retry_at = next_retry_at
        message.last_error = error

    def mark_failed(self, message: Outbox, *, attempts: int, error: str) -> None:
        message.status = OutboxStatus.FAILED
        message.attempts = attempts
        message.last_error = error
        message.locked_until = None
        message.next_retry_at = None
