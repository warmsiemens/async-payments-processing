from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException

from app.api.dependencies import SessionDep
from app.core.exceptions import IdempotencyConflict
from app.models.payment import Payment
from app.schemas.payment import PaymentAccepted, PaymentCreate, PaymentDetail
from app.services.payments import create_payment

router = APIRouter(prefix="/payments", tags=["payments"])


@router.post("", status_code=202, response_model=PaymentAccepted)
async def post_payment(
    body: PaymentCreate,
    session: SessionDep,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^\S+$")
    ],
):
    try:
        async with session.begin():
            payment = await create_payment(session, body, idempotency_key)
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return PaymentAccepted(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


@router.get("/{payment_id}", response_model=PaymentDetail)
async def get_payment(payment_id: UUID, session: SessionDep):
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return PaymentDetail(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_json,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        processed_at=payment.processed_at,
        webhook_delivered_at=payment.webhook_delivered_at,
    )
