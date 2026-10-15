"""Async token bucket rate limiter."""

import asyncio
import time
from typing import Optional


class AsyncTokenBucket:
    """Thread-safe async token bucket rate limiter."""

    def __init__(self, rate_limit_rps: float, capacity: Optional[float] = None):
        self.rate = rate_limit_rps  # tokens per second
        self.capacity = capacity if capacity is not None else rate_limit_rps
        self.tokens = self.capacity
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> None:
        """Wait until enough tokens are available."""
        async with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.last_update = now

                # Add tokens based on elapsed time
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

                # Calculate wait time for needed tokens
                needed = tokens - self.tokens
                wait_seconds = needed / self.rate
                await asyncio.sleep(wait_seconds)
