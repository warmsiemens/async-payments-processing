"""Import all ORM models to register their tables in Base.metadata for Alembic."""

from app.models.outbox import Outbox
from app.models.payment import Payment

__all__ = ["Outbox", "Payment"]
