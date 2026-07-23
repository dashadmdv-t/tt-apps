import asyncio
import logging

from sqlalchemy.exc import SQLAlchemyError

from src.container import Container

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

DISPATCH_BATCH_SIZE = 100
DISPATCH_POLL_INTERVAL_SECONDS = 1.0
DISPATCH_ERROR_BACKOFF_SECONDS = 3.0
RETRYABLE_DISPATCH_ERRORS = (SQLAlchemyError, RuntimeError, OSError, TimeoutError)

container = Container()
messaging = container.messaging()
dispatcher = container.outbox_dispatcher_service()


async def run_outbox_dispatcher() -> None:
    async with messaging.broker:
        await messaging.declare_topology()
        logger.info("Outbox dispatcher started")
        while True:
            try:
                stats = await dispatcher.dispatch_pending(limit=DISPATCH_BATCH_SIZE)
                if stats["selected"] > 0:
                    logger.info("Outbox dispatch stats: %s", stats)
                await asyncio.sleep(DISPATCH_POLL_INTERVAL_SECONDS)
            except RETRYABLE_DISPATCH_ERRORS:
                logger.exception("Outbox dispatcher iteration failed")
                await asyncio.sleep(DISPATCH_ERROR_BACKOFF_SECONDS)


if __name__ == "__main__":
    asyncio.run(run_outbox_dispatcher())
