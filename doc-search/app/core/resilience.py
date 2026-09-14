"""Small, dependency-free resilience primitives: retry + circuit breaker.

Deliberately minimal. The goal is to demonstrate the failure-isolation pattern
that keeps the API responsive when OpenSearch degrades, not to reimplement a
service mesh. In production these live in the mesh/sidecar (Envoy outlier
detection) or a library such as `pybreaker`.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import TypeVar

from app.core.errors import DependencyUnavailableError
from app.core.metrics import circuit_state

logger = logging.getLogger(__name__)
T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


_STATE_VALUE = {CircuitState.CLOSED: 0, CircuitState.HALF_OPEN: 1, CircuitState.OPEN: 2}


class CircuitBreaker:
    """Fail fast when a dependency is consistently failing.

    CLOSED -> (N consecutive failures) -> OPEN -> (after recovery window) ->
    HALF_OPEN -> (1 success) -> CLOSED | (1 failure) -> OPEN.
    """

    def __init__(self, name: str, failure_threshold: int = 5, recovery_seconds: float = 10.0) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self._failures = 0
        self._opened_at = 0.0
        self._state = CircuitState.CLOSED
        self._lock = asyncio.Lock()
        circuit_state.labels(dependency=name).set(0)

    @property
    def state(self) -> CircuitState:
        return self._state

    def _set_state(self, state: CircuitState) -> None:
        self._state = state
        circuit_state.labels(dependency=self.name).set(_STATE_VALUE[state])

    async def _before_call(self) -> None:
        async with self._lock:
            if self._state is CircuitState.OPEN:
                if time.monotonic() - self._opened_at >= self.recovery_seconds:
                    self._set_state(CircuitState.HALF_OPEN)
                    logger.warning("circuit half-open", extra={"dependency": self.name})
                else:
                    raise DependencyUnavailableError(self.name)

    async def _on_success(self) -> None:
        async with self._lock:
            self._failures = 0
            if self._state is not CircuitState.CLOSED:
                logger.info("circuit closed", extra={"dependency": self.name})
            self._set_state(CircuitState.CLOSED)

    async def _on_failure(self) -> None:
        async with self._lock:
            self._failures += 1
            if self._state is CircuitState.HALF_OPEN or self._failures >= self.failure_threshold:
                self._opened_at = time.monotonic()
                self._set_state(CircuitState.OPEN)
                logger.error("circuit opened", extra={"dependency": self.name, "failures": self._failures})

    async def call(self, fn: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        await self._before_call()
        try:
            result = await fn(*args, **kwargs)
        except DependencyUnavailableError:
            raise
        except Exception:
            await self._on_failure()
            raise
        await self._on_success()
        return result


async def with_retry(
    fn: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    base_delay: float = 0.05,
    max_delay: float = 0.5,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    operation: str = "call",
) -> T:
    """Retry with exponential backoff + full jitter (avoids synchronised retry storms)."""
    last_exc: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await fn()
        except retry_on as exc:  # noqa: PERF203
            last_exc = exc
            if attempt == attempts:
                break
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            await asyncio.sleep(random.uniform(0, delay))
            logger.warning("retrying operation", extra={"operation": operation, "attempt": attempt})
    assert last_exc is not None
    raise last_exc
