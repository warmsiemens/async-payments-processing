from datetime import datetime
from typing import Literal
from uuid import UUID, uuid4

from app.core.time import utcnow
from app.messaging.constants import PAYMENTS_QUEUE
from app.models.outbox import Outbox
from app.schemas.events import PaymentEvent


def new_event(
    payment_id: UUID,
    attempt: int = 1,
    available_at: datetime | None = None,
    *,
    stage: Literal["processing", "webhook"] = "processing",
) -> Outbox:
    event_id = uuid4()
    event = PaymentEvent(event_id=event_id, payment_id=payment_id, attempt=attempt, stage=stage)
    return Outbox(
        id=event_id,
        payment_id=payment_id,
        routing_key=PAYMENTS_QUEUE,
        payload=event.model_dump(mode="json"),
        available_at=available_at or utcnow(),
    )
