from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.api.dependencies import DatabaseDep

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(database: DatabaseDep):
    try:
        async with database.sessions() as session:
            await session.execute(text("SELECT 1"))
    except Exception:
        return JSONResponse(status_code=503, content={"status": "unavailable"})
    return {"status": "ok"}
