import asyncio
import json
import logging
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID

from src.infrastructure.messaging.rabbit import PaymentMessaging

logger = logging.getLogger(__name__)

WEBHOOK_MAX_ATTEMPTS = 3
WEBHOOK_BASE_DELAY_SECONDS = 2
RETRYABLE_WEBHOOK_ERRORS = (RuntimeError, OSError, TimeoutError)

DeliveryStatus = Literal["delivered", "skipped", "dlq_published", "dlq_publish_failed"]


class WebhookDeliveryService:
    def __init__(self, messaging: PaymentMessaging):
        self._messaging = messaging

    async def _send_once(self, url: str, payload: dict[str, Any]) -> None:
        def _post() -> None:
            request = Request(
                url=url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(request, timeout=10) as response:
                if response.status >= 400:
                    raise RuntimeError(f"Webhook response status: {response.status}")

        try:
            await asyncio.to_thread(_post)
        except HTTPError as error:
            raise RuntimeError(f"Webhook HTTP error: {error.code}") from error
        except URLError as error:
            raise RuntimeError(f"Webhook URL error: {error.reason}") from error

    async def _send_with_retry(self, url: str, payload: dict[str, Any]) -> None:
        last_error: Exception | None = None
        for attempt in range(1, WEBHOOK_MAX_ATTEMPTS + 1):
            try:
                await self._send_once(url, payload)
                return
            except RETRYABLE_WEBHOOK_ERRORS as error:
                last_error = error
                if attempt >= WEBHOOK_MAX_ATTEMPTS:
                    break
                delay_seconds = WEBHOOK_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "Webhook attempt %s failed, retry in %ss: %s",
                    attempt,
                    delay_seconds,
                    error,
                )
                await asyncio.sleep(delay_seconds)
        raise RuntimeError(str(last_error) if last_error else "Webhook failed")

    async def deliver(
        self,
        *,
        payment_id: UUID,
        webhook_url: str | None,
        payload: dict[str, Any],
    ) -> DeliveryStatus:
        if not webhook_url:
            logger.info("Webhook URL is empty, payment %s completed without callback", payment_id)
            return "skipped"

        try:
            await self._send_with_retry(webhook_url, payload)
            return "delivered"
        except RETRYABLE_WEBHOOK_ERRORS as error:
            logger.exception("Webhook delivery failed for payment %s", payment_id)
            try:
                await self._messaging.publish_payment_dlq(
                    {
                        "payment_id": str(payment_id),
                        "reason": f"webhook_failed: {error}",
                        "payload": payload,
                    },
                    message_id=str(payment_id),
                )
                return "dlq_published"
            except RETRYABLE_WEBHOOK_ERRORS:
                logger.exception("DLQ publish failed for payment %s", payment_id)
                return "dlq_publish_failed"
