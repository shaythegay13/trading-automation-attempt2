"""Unit tests for token bucket rate limiter."""

import time

import pytest

from src.clients.rate_limiter import AsyncTokenBucket


@pytest.mark.asyncio
async def test_token_bucket_acquire():
    limiter = AsyncTokenBucket(rate_limit_rps=20.0, capacity=2.0)
    start = time.monotonic()
    await limiter.acquire(1.0)
    await limiter.acquire(1.0)
    elapsed = time.monotonic() - start
    assert elapsed < 0.2  # Should consume burst capacity instantly


@pytest.mark.asyncio
async def test_token_bucket_throttle():
    limiter = AsyncTokenBucket(rate_limit_rps=10.0, capacity=1.0)
    await limiter.acquire(1.0)
    start = time.monotonic()
    await limiter.acquire(1.0)  # Needs to wait ~0.1s
    elapsed = time.monotonic() - start
    assert elapsed >= 0.08
