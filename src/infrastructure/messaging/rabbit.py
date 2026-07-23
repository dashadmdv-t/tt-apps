from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue

from src.core.config import AppSettings

PAYMENTS_EXCHANGE_NAME = "payments"
PAYMENTS_NEW_ROUTING_KEY = "payments.new"
PAYMENTS_NEW_QUEUE_NAME = "payments.new"
PAYMENTS_DLX_EXCHANGE_NAME = "payments.dlx"
PAYMENTS_RETRY_1_ROUTING_KEY = "payments.new.retry.1"
PAYMENTS_RETRY_2_ROUTING_KEY = "payments.new.retry.2"
PAYMENTS_RETRY_1_QUEUE_NAME = "payments.new.retry.1"
PAYMENTS_RETRY_2_QUEUE_NAME = "payments.new.retry.2"
PAYMENTS_NEW_DLQ_ROUTING_KEY = "payments.new.dlq"
PAYMENTS_NEW_DLQ_QUEUE_NAME = "payments.new.dlq"
PAYMENTS_RETRY_1_DELAY_MS = 2000
PAYMENTS_RETRY_2_DELAY_MS = 4000


class PaymentMessaging:
    def __init__(self, settings: AppSettings):
        self.broker = RabbitBroker(settings.rabbit_url)
        self.exchange = RabbitExchange(PAYMENTS_EXCHANGE_NAME, durable=True)
        self.dead_letter_exchange = RabbitExchange(PAYMENTS_DLX_EXCHANGE_NAME, durable=True)
        self.new_queue = RabbitQueue(
            PAYMENTS_NEW_QUEUE_NAME,
            durable=True,
            routing_key=PAYMENTS_NEW_ROUTING_KEY,
            arguments={
                "x-dead-letter-exchange": PAYMENTS_DLX_EXCHANGE_NAME,
                "x-dead-letter-routing-key": PAYMENTS_RETRY_1_ROUTING_KEY,
            },
        )
        self.retry_queue_1 = RabbitQueue(
            PAYMENTS_RETRY_1_QUEUE_NAME,
            durable=True,
            routing_key=PAYMENTS_RETRY_1_ROUTING_KEY,
            arguments={
                "x-message-ttl": PAYMENTS_RETRY_1_DELAY_MS,
                "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
                "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
            },
        )
        self.retry_queue_2 = RabbitQueue(
            PAYMENTS_RETRY_2_QUEUE_NAME,
            durable=True,
            routing_key=PAYMENTS_RETRY_2_ROUTING_KEY,
            arguments={
                "x-message-ttl": PAYMENTS_RETRY_2_DELAY_MS,
                "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
                "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
            },
        )
        self.dlq_queue = RabbitQueue(
            PAYMENTS_NEW_DLQ_QUEUE_NAME,
            durable=True,
            routing_key=PAYMENTS_NEW_DLQ_ROUTING_KEY,
        )

    async def declare_topology(self) -> None:
        exchange = await self.broker.declare_exchange(self.exchange)
        dlx_exchange = await self.broker.declare_exchange(self.dead_letter_exchange)

        new_queue = await self.broker.declare_queue(self.new_queue)
        retry_queue_1 = await self.broker.declare_queue(self.retry_queue_1)
        retry_queue_2 = await self.broker.declare_queue(self.retry_queue_2)
        dlq_queue = await self.broker.declare_queue(self.dlq_queue)

        await new_queue.bind(exchange=exchange, routing_key=PAYMENTS_NEW_ROUTING_KEY)
        await retry_queue_1.bind(exchange=dlx_exchange, routing_key=PAYMENTS_RETRY_1_ROUTING_KEY)
        await retry_queue_2.bind(exchange=dlx_exchange, routing_key=PAYMENTS_RETRY_2_ROUTING_KEY)
        await dlq_queue.bind(exchange=dlx_exchange, routing_key=PAYMENTS_NEW_DLQ_ROUTING_KEY)

    async def publish_payment_new(self, message: dict, *, message_id: str | None = None) -> None:
        await self.broker.publish(
            message=message,
            exchange=self.exchange,
            queue=self.new_queue,
            routing_key=PAYMENTS_NEW_ROUTING_KEY,
            message_id=message_id,
            persist=True,
            mandatory=True,
        )

    async def publish_payment_retry(
        self,
        message: dict,
        *,
        attempt: int,
        message_id: str | None = None,
    ) -> None:
        if attempt == 2:
            queue = self.retry_queue_1
            routing_key = PAYMENTS_RETRY_1_ROUTING_KEY
        elif attempt == 3:
            queue = self.retry_queue_2
            routing_key = PAYMENTS_RETRY_2_ROUTING_KEY
        else:
            raise ValueError(f"Unsupported retry attempt: {attempt}")

        await self.broker.publish(
            message=message,
            exchange=self.dead_letter_exchange,
            queue=queue,
            routing_key=routing_key,
            message_id=message_id,
            persist=True,
            mandatory=True,
        )

    async def publish_payment_dlq(self, message: dict, *, message_id: str | None = None) -> None:
        await self.broker.publish(
            message=message,
            exchange=self.dead_letter_exchange,
            queue=self.dlq_queue,
            routing_key=PAYMENTS_NEW_DLQ_ROUTING_KEY,
            message_id=message_id,
            persist=True,
            mandatory=True,
        )
