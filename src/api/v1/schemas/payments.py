from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from src.core.enums import Currency, PaymentStatus

Amount = Annotated[
    Decimal,
    Field(gt=0, max_digits=18, decimal_places=2),
]


class PaymentGET(BaseModel):
    payment_id: UUID = Field(default_factory=uuid4)
    amount: Amount
    currency: Currency
    description: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    status: PaymentStatus = PaymentStatus.PENDING
    webhook_url: HttpUrl | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    processed_at: datetime | None = None
    model_config = ConfigDict(from_attributes=True)


class PaymentPOST(BaseModel):
    amount: Amount
    currency: Currency
    description: str | None = Field(default=None, max_length=255)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl | None = None


class PaymentCreated(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime
