import asyncio
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from sqlalchemy.ext.asyncio import async_sessionmaker

from src.core.enums import PaymentStatus
from src.db.uow.payments import PaymentsUnitOfWork

ProcessingState = Literal["processed", "already_processed", "not_found", "failed"]


class PaymentProcessingRetryableError(RuntimeError):
    pass


@dataclass(slots=True)
class PaymentProcessingResult:
    state: ProcessingState
    payment_id: UUID
    webhook_url: str | None = None
    webhook_payload: dict[str, Any] | None = None


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _webhook_body(payment_id: UUID, status: str, processed_at: datetime | None) -> dict[str, Any]:
    return {
        "payment_id": str(payment_id),
        "status": status,
        "processed_at": processed_at.isoformat() if processed_at else None,
    }


class PaymentProcessingService:
    def __init__(self, session_factory: async_sessionmaker):
        self._session_factory = session_factory

    async def process_created(
        self,
        payment_id: UUID,
        *,
        final_attempt: bool = False,
    ) -> PaymentProcessingResult:
        await asyncio.sleep(random.uniform(2, 5))

        async with self._session_factory() as session:
            async with PaymentsUnitOfWork(session) as uow:
                payment = await uow.payments.get_by_id_for_update(payment_id)
                if payment is None:
                    return PaymentProcessingResult(state="not_found", payment_id=payment_id)

                if payment.processed_at is not None:
                    return PaymentProcessingResult(
                        state="already_processed",
                        payment_id=payment_id,
                        webhook_url=payment.webhook_url,
                        webhook_payload=_webhook_body(
                            payment_id=payment.payment_id,
                            status=payment.status.value,
                            processed_at=payment.processed_at,
                        ),
                    )

                # эмуляция 90% успеха и 10% фейла
                should_succeed = random.choices([True, False], weights=[0.9, 0.1])[0]
                if not should_succeed:
                    if not final_attempt:
                        raise PaymentProcessingRetryableError(
                            f"Temporary payment processing error for {payment_id}"
                        )

                    payment.status = PaymentStatus.FAILED
                    payment.processed_at = _utc_now()
                    await session.commit()

                    return PaymentProcessingResult(
                        state="failed",
                        payment_id=payment.payment_id,
                        webhook_url=payment.webhook_url,
                        webhook_payload=_webhook_body(
                            payment_id=payment.payment_id,
                            status=payment.status.value,
                            processed_at=payment.processed_at,
                        ),
                    )

                payment.status = PaymentStatus.SUCCEEDED
                payment.processed_at = _utc_now()
                await session.commit()

                return PaymentProcessingResult(
                    state="processed",
                    payment_id=payment.payment_id,
                    webhook_url=payment.webhook_url,
                    webhook_payload=_webhook_body(
                        payment_id=payment.payment_id,
                        status=payment.status.value,
                        processed_at=payment.processed_at,
                    ),
                )
