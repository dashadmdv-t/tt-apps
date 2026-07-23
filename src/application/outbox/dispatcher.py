from datetime import UTC, datetime, timedelta

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.application.payments.service import PAYMENTS_NEW_TOPIC
from src.db.uow.payments import PaymentsUnitOfWork
from src.infrastructure.messaging.rabbit import PaymentMessaging

MAX_OUTBOX_ATTEMPTS = 3
BASE_RETRY_DELAY_SECONDS = 2
CLAIM_LEASE_SECONDS = 300
RETRYABLE_OUTBOX_ERRORS = (SQLAlchemyError, RuntimeError, OSError, TimeoutError)


def _now() -> datetime:
    return datetime.now(UTC)


def _next_retry(attempts: int, *, now: datetime | None = None) -> datetime:
    current = now or _now()
    delay_seconds = BASE_RETRY_DELAY_SECONDS * (2 ** max(attempts - 1, 0))
    return current + timedelta(seconds=delay_seconds)


class OutboxDispatcherService:
    def __init__(self, session_factory: async_sessionmaker, messaging: PaymentMessaging):
        self._session_factory = session_factory
        self._messaging = messaging

    async def dispatch_pending(self, *, limit: int = 100) -> dict[str, int]:
        sent_count = 0
        failed_count = 0
        messages = []

        async with self._session_factory() as session:
            async with PaymentsUnitOfWork(session) as uow:
                messages = await uow.outbox.get_ready_for_dispatch(now=_now(), limit=limit)
                claim_until = _now() + timedelta(seconds=CLAIM_LEASE_SECONDS)
                for message in messages:
                    uow.outbox.claim_for_dispatch(message, locked_until=claim_until)
                if messages:
                    await session.commit()
                for message in messages:
                    try:
                        if message.topic != PAYMENTS_NEW_TOPIC:
                            error = f"Unsupported outbox topic: {message.topic}"
                            uow.outbox.mark_failed(
                                message,
                                attempts=message.attempts + 1,
                                error=error,
                            )
                            failed_count += 1
                            try:
                                await self._messaging.publish_payment_dlq(
                                    {
                                        "reason": "unsupported_topic",
                                        "topic": message.topic,
                                        "payload": message.payload,
                                    },
                                    message_id=str(message.id),
                                )
                            except RETRYABLE_OUTBOX_ERRORS as dlq_error:
                                message.last_error = f"{error}; dlq_error={str(dlq_error)[:2000]}"
                            await session.commit()
                            continue
                        await self._messaging.publish_payment_new(
                            message.payload,
                            message_id=str(message.id),
                        )
                        uow.outbox.mark_published(message, now=_now())
                        sent_count += 1
                        await session.commit()
                    except RETRYABLE_OUTBOX_ERRORS as publish_error:
                        attempts = message.attempts + 1
                        error = str(publish_error)[:2000]

                        if attempts >= MAX_OUTBOX_ATTEMPTS:
                            uow.outbox.mark_failed(message, attempts=attempts, error=error)
                            failed_count += 1
                            try:
                                await self._messaging.publish_payment_dlq(
                                    message.payload,
                                    message_id=str(message.id),
                                )
                            except RETRYABLE_OUTBOX_ERRORS as dlq_error:
                                message.last_error = f"{error}; dlq_error={str(dlq_error)[:2000]}"
                        else:
                            uow.outbox.schedule_retry(
                                message,
                                attempts=attempts,
                                next_retry_at=_next_retry(attempts),
                                error=error,
                            )
                        await session.commit()

        return {"selected": len(messages), "sent": sent_count, "failed": failed_count}
