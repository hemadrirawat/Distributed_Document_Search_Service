from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


def utcnow() -> datetime:
    return datetime.now(UTC)


def as_utc(value: datetime) -> datetime:
    """Normalise to timezone-aware UTC (SQLite returns naive datetimes)."""
    return value if value.tzinfo else value.replace(tzinfo=UTC)
