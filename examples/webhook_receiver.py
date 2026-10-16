"""Local demo callback receiver. This separate client app is not part of the payment API."""

import logging

from fastapi import FastAPI, Header, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI(title="Demo webhook receiver")
received: set[str] = set()
logger = logging.getLogger("uvicorn.error")


class Callback(BaseModel):
    payment_id: str
    status: str


@app.post("/webhook")
async def receive(
    body: Callback,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    status_code: int = Query(default=200, ge=200, le=599),
):
    if status_code >= 300:
        logger.info("Simulated webhook error: payment=%s status_code=%s", body.payment_id, status_code)
        return JSONResponse(status_code=status_code, content={"received": False})
    duplicate = idempotency_key in received
    received.add(idempotency_key)
    logger.info("Webhook payment=%s status=%s duplicate=%s", body.payment_id, body.status, duplicate)
    return {"received": True, "duplicate": duplicate}
