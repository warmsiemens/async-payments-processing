from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, HTTPException, Request
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.security import api_key_matches
from app.db.session import Database

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(request: Request, key: Annotated[str | None, Depends(api_key_header)]) -> None:
    expected = request.app.state.settings.api_key.get_secret_value()
    if not api_key_matches(key, expected):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def get_database(request: Request) -> Database:
    return request.app.state.database


DatabaseDep = Annotated[Database, Depends(get_database)]


async def get_session(database: DatabaseDep) -> AsyncIterator[AsyncSession]:
    async with database.sessions() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
