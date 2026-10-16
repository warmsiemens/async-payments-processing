from fastapi import APIRouter

from app.api.v1.payments import router as payments_router

router = APIRouter(prefix="/api/v1")
router.include_router(payments_router)
