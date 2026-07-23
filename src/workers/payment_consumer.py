import asyncio
import logging
from typing import Any
from uuid import UUID

from faststream import AckPolicy, FastStream
from faststream.rabbit.annotations import RabbitMessage
from sqlalchemy.exc import SQLAlchemyError

from src.application.payments.processing import PaymentProcessingRetryableError
from src.container import Container

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

container = Container()
messaging = container.messaging()
payment_processing_service = container.payment_processing_service()
webhook_delivery_service = container.webhook_delivery_service()
app = FastStream(messaging.broker)
MAX_CONSUMER_ATTEMPTS = 3
RETRYABLE_CONSUMER_ERRORS = (
    SQLAlchemyError,
    RuntimeError,
    OSError,
    TimeoutError,
    PaymentProcessingRetryableError,
)


def _message_attempt(message: dict[str, Any]) -> int:
    attempt = message.get("attempt", 1)
    if isinstance(attempt, int) and attempt >= 1:
        return attempt
    if isinstance(attempt, str) and attempt.isdigit():
        return max(int(attempt), 1)
    return 1


@app.after_startup
async def startup_declarations() -> None:
    await messaging.broker.connect()
    await messaging.declare_topology()


@app.after_shutdown
async def shutdown_broker() -> None:
    close = getattr(messaging.broker, "close", None)
    if callable(close):
        await close()


@messaging.broker.subscriber(messaging.new_queue, messaging.exchange, ack_policy=AckPolicy.MANUAL)
async def handle_payment_created(message: dict, msg: RabbitMessage) -> None:
    payment_id_raw = message.get("payment_id")
    if not payment_id_raw:
        logger.warning("Skip message without payment_id: %s", message)
        try:
            await messaging.publish_payment_dlq(
                {
                    "reason": "missing_payment_id",
                    "payload": message,
                },
            )
            await msg.ack()
        except RETRYABLE_CONSUMER_ERRORS:
            logger.exception("DLQ publish failed for malformed message without payment_id")
            await msg.nack(requeue=False)
        return

    try:
        payment_id = UUID(payment_id_raw)
    except ValueError:
        logger.warning("Skip message with invalid payment_id: %s", message)
        try:
            await messaging.publish_payment_dlq(
                {
                    "reason": "invalid_payment_id",
                    "payload": message,
                },
                message_id=payment_id_raw,
            )
            await msg.ack()
        except RETRYABLE_CONSUMER_ERRORS:
            logger.exception("DLQ publish failed for invalid payment_id message")
            await msg.nack(requeue=False)
        return

    attempt = _message_attempt(message)
    if attempt > MAX_CONSUMER_ATTEMPTS:
        logger.error("Retries exhausted for payment %s", payment_id)
        try:
            await messaging.publish_payment_dlq(
                {
                    "payment_id": str(payment_id),
                    "attempt": attempt,
                    "reason": "consumer_retries_exhausted",
                    "payload": message,
                },
                message_id=str(payment_id),
            )
            await msg.ack()
        except RETRYABLE_CONSUMER_ERRORS:
            logger.exception(
                "DLQ publish failed after retries exhausted for payment %s", payment_id
            )
            await msg.nack(requeue=False)
        return

    try:
        result = await payment_processing_service.process_created(
            payment_id=payment_id,
            final_attempt=attempt >= MAX_CONSUMER_ATTEMPTS,
        )
        publish_to_dlq = False
        dlq_reason: str | None = None
        if result.state == "not_found":
            logger.warning("Payment not found for message: %s", payment_id)
            await msg.ack()
            return
        if result.state == "already_processed":
            logger.info("Payment already processed: %s", payment_id)
        if result.state == "failed":
            logger.warning("Payment failed on final attempt: %s", payment_id)
            publish_to_dlq = True
            dlq_reason = "consumer_processing_failed"

        delivery_status = await webhook_delivery_service.deliver(
            payment_id=result.payment_id,
            webhook_url=result.webhook_url,
            payload=result.webhook_payload or {},
        )
        if delivery_status == "dlq_publish_failed":
            raise RuntimeError("Webhook DLQ publish failed")

        if publish_to_dlq:
            await messaging.publish_payment_dlq(
                {
                    **message,
                    "attempt": attempt,
                    "payment_id": str(payment_id),
                    "reason": dlq_reason,
                },
                message_id=str(payment_id),
            )
            logger.error("Published payment %s to DLQ after final failure", payment_id)

        await msg.ack()
    except RETRYABLE_CONSUMER_ERRORS:
        logger.exception(
            "Temporary processing error for payment %s on attempt %s",
            payment_id,
            attempt,
        )
        next_attempt = attempt + 1
        try:
            if next_attempt > MAX_CONSUMER_ATTEMPTS:
                logger.error(
                    "Retries exhausted for payment %s after attempt %s",
                    payment_id,
                    attempt,
                )
                await messaging.publish_payment_dlq(
                    {
                        **message,
                        "attempt": next_attempt,
                        "payment_id": str(payment_id),
                        "reason": "consumer_processing_failed",
                    },
                    message_id=str(payment_id),
                )
            else:
                logger.info(
                    "Retry payment %s with attempt %s of %s",
                    payment_id,
                    next_attempt,
                    MAX_CONSUMER_ATTEMPTS,
                )
                await messaging.publish_payment_retry(
                    {
                        **message,
                        "attempt": next_attempt,
                        "payment_id": str(payment_id),
                        "reason": "consumer_processing_failed",
                    },
                    attempt=next_attempt,
                    message_id=str(payment_id),
                )
            await msg.ack()
        except RETRYABLE_CONSUMER_ERRORS:
            logger.exception("Retry publish failed for payment %s", payment_id)
            await msg.nack(requeue=False)


if __name__ == "__main__":
    asyncio.run(app.run())
