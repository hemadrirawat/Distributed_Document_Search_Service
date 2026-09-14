from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, utcnow


class Tenant(Base):
    """Tenant registry. Owns the credential material and the per-tenant quota.

    Rate limits live here (not in config) so limits are per-tenant and can be
    changed at runtime without a deploy.
    """

    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    # Only the hash is persisted; the raw API key is never stored.
    api_key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    rate_limit_per_minute: Mapped[int] = mapped_column(Integer, nullable=False, default=120)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
