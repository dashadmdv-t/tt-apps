from dependency_injector import containers, providers

from src.application.outbox.dispatcher import OutboxDispatcherService
from src.application.payments.processing import PaymentProcessingService
from src.application.payments.service import PaymentService
from src.application.webhook.service import WebhookDeliveryService
from src.core.config import AppSettings
from src.infrastructure.db.database import Database
from src.infrastructure.messaging.rabbit import PaymentMessaging


class Container(containers.DeclarativeContainer):
    settings = providers.Singleton(AppSettings)

    database = providers.Singleton(Database, settings=settings)
    messaging = providers.Singleton(PaymentMessaging, settings=settings)

    session_factory = providers.Callable(lambda database: database.session_factory, database)

    payment_service = providers.Factory(PaymentService, session_factory=session_factory)
    payment_processing_service = providers.Factory(
        PaymentProcessingService,
        session_factory=session_factory,
    )
    webhook_delivery_service = providers.Factory(
        WebhookDeliveryService,
        messaging=messaging,
    )
    outbox_dispatcher_service = providers.Factory(
        OutboxDispatcherService,
        session_factory=session_factory,
        messaging=messaging,
    )
