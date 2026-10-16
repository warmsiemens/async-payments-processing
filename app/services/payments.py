import hashlib
import json
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import IdempotencyConflict
from app.models.payment import Payment
from app.schemas.payment import PaymentCreate
from app.services.events import new_event


async def create_payment(session: AsyncSession, body: PaymentCreate, key: str) -> Payment:
    payload = body.model_dump(mode="json")
    fingerprint = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    payment = (
        await session.execute(
            insert(Payment)
            .values(
                id=uuid4(),
                amount=body.amount,
                currency=body.currency,
                description=body.description,
                metadata_json=body.metadata,
                webhook_url=str(body.webhook_url),
                idempotency_key=key,
                request_hash=fingerprint,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
    ).scalar_one_or_none()
    if payment is None:
        payment = (await session.execute(select(Payment).where(Payment.idempotency_key == key))).scalar_one()
        if payment.request_hash != fingerprint:
            raise IdempotencyConflict("Idempotency-Key is already used for a different request")
    else:
        session.add(new_event(payment.id))
    return payment
