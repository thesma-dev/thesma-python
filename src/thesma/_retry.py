"""Rate-limit retry logic for 429 responses."""

from __future__ import annotations

import asyncio
import random
import time
from typing import TYPE_CHECKING, TypeVar

from thesma.errors import RateLimitError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

T = TypeVar("T")

DEFAULT_RETRY_AFTER: float = 1.0


def sync_retry(fn: Callable[[], T], max_retries: int) -> T:
    """Retry *fn* on :class:`RateLimitError` using ``time.sleep``.

    *max_retries* is the number of **retries** (total attempts = max_retries + 1).
    On exhaustion the last :class:`RateLimitError` is re-raised.
    """
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except RateLimitError as exc:
            if attempt >= max_retries:
                raise
            retry_after = exc.retry_after if exc.retry_after is not None else DEFAULT_RETRY_AFTER
            sleep_time = retry_after + random.uniform(0, 0.5)
            time.sleep(sleep_time)
    raise RuntimeError("unreachable")  # pragma: no cover


async def async_retry(fn: Callable[[], Awaitable[T]], max_retries: int) -> T:
    """Retry *fn* on :class:`RateLimitError` using ``asyncio.sleep``.

    *max_retries* is the number of **retries** (total attempts = max_retries + 1).
    On exhaustion the last :class:`RateLimitError` is re-raised.
    """
    for attempt in range(max_retries + 1):
        try:
            return await fn()
        except RateLimitError as exc:
            if attempt >= max_retries:
                raise
            retry_after = exc.retry_after if exc.retry_after is not None else DEFAULT_RETRY_AFTER
            sleep_time = retry_after + random.uniform(0, 0.5)
            await asyncio.sleep(sleep_time)
    raise RuntimeError("unreachable")  # pragma: no cover


__all__ = ["DEFAULT_RETRY_AFTER", "async_retry", "sync_retry"]
