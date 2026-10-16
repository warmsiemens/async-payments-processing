import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from app.api.dependencies import require_api_key
from app.api.health import router as health_router
from app.api.middleware import APIKeyMiddleware
from app.api.router import router as api_router
from app.core.config import Settings, get_settings
from app.db.session import Database


def create_app(settings: Settings | None = None, database: Database | None = None) -> FastAPI:
    settings = settings or get_settings()
    logging.basicConfig(level=settings.log_level)
    database = database or Database(settings.database_url)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        try:
            yield
        finally:
            await database.close()

    app = FastAPI(
        title="Async payment processing",
        version="1.0.0",
        lifespan=lifespan,
        dependencies=[Depends(require_api_key)],
    )
    app.state.settings = settings
    app.state.database = database
    app.add_middleware(APIKeyMiddleware, api_key=settings.api_key.get_secret_value())
    app.include_router(health_router)
    app.include_router(api_router)
    return app
