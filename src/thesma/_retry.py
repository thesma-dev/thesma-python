"""Retry logic for transient errors (429, 5xx, connection, timeout)."""

from __future__ import annotations

import asyncio
import random
import time
from typing import TYPE_CHECKING, TypeVar

from thesma.errors import ConnectionError, RateLimitError, ServerError, TimeoutError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

T = TypeVar("T")

DEFAULT_RETRY_AFTER: float = 1.0

_RETRYABLE = (RateLimitError, ServerError, ConnectionError, TimeoutError)


def _compute_sleep(exc: Exception, attempt: int) -> float:
    """Compute sleep duration for a retryable error.

    *attempt* is 0-based (the attempt that just failed).
    """
    if isinstance(exc, RateLimitError):
        retry_after = exc.retry_after if exc.retry_after is not None else DEFAULT_RETRY_AFTER
        return retry_after + random.uniform(0, 0.5)
    return min(2**attempt, 30) + random.uniform(0, 0.5)


def sync_retry(fn: Callable[[], T], max_retries: int) -> T:
    """Retry *fn* on transient errors using ``time.sleep``.

    *max_retries* is the number of **retries** (total attempts = max_retries + 1).
    On exhaustion the last exception is re-raised.
    """
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except _RETRYABLE as exc:
            if attempt >= max_retries:
                raise
            sleep_time = _compute_sleep(exc, attempt)
            time.sleep(sleep_time)
    raise RuntimeError("unreachable")  # pragma: no cover


async def async_retry(fn: Callable[[], Awaitable[T]], max_retries: int) -> T:
    """Retry *fn* on transient errors using ``asyncio.sleep``.

    *max_retries* is the number of **retries** (total attempts = max_retries + 1).
    On exhaustion the last exception is re-raised.
    """
    for attempt in range(max_retries + 1):
        try:
            return await fn()
        except _RETRYABLE as exc:
            if attempt >= max_retries:
                raise
            sleep_time = _compute_sleep(exc, attempt)
            await asyncio.sleep(sleep_time)
    raise RuntimeError("unreachable")  # pragma: no cover


__all__ = ["DEFAULT_RETRY_AFTER", "async_retry", "sync_retry"]
