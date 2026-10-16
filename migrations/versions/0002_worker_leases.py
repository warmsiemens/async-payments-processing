"""Add worker leases and a separate webhook attempt counter.

Revision ID: 0002
Revises: 0001
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("payments", sa.Column("webhook_attempts", sa.Integer(), nullable=False, server_default="0"))
    op.create_check_constraint("ck_payments_webhook_attempts", "payments", "webhook_attempts BETWEEN 0 AND 3")
    op.add_column("payments", sa.Column("worker_token", postgresql.UUID(as_uuid=True)))
    op.add_column("payments", sa.Column("worker_lease_until", sa.DateTime(timezone=True)))
    op.add_column("outbox", sa.Column("publish_token", postgresql.UUID(as_uuid=True)))
    op.add_column("outbox", sa.Column("publish_lease_until", sa.DateTime(timezone=True)))


def downgrade() -> None:
    op.drop_column("outbox", "publish_lease_until")
    op.drop_column("outbox", "publish_token")
    op.drop_column("payments", "worker_lease_until")
    op.drop_column("payments", "worker_token")
    op.drop_constraint("ck_payments_webhook_attempts", "payments", type_="check")
    op.drop_column("payments", "webhook_attempts")
