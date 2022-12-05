import asyncio
import time
from typing import Set
from unittest import mock

import pytest

from cbasyncio.limits import AsyncLimiter

WAIT_LIMIT = 2


async def wait_for_n_done(tasks, n) -> Set:
    """Wait for n (or more) tasks to have completed"""
    start = time.time()
    pending = tasks
    remainder = len(tasks) - n
    while time.time() <= start + WAIT_LIMIT and len(pending) > remainder:
        done, pending = await asyncio.wait(tasks, timeout=WAIT_LIMIT, return_when=asyncio.FIRST_COMPLETED)
    assert len(pending) >= remainder
    return pending


def test_attributes():
    limiter = AsyncLimiter(42, 81)
    assert limiter.max_rate == 42
    assert limiter.time_period == 81


@pytest.mark.asyncio
async def test_has_capacity():
    limiter = AsyncLimiter(1)
    assert limiter.has_capacity()
    assert not limiter.has_capacity(42)

    await limiter.acquire()
    assert not limiter.has_capacity()


@pytest.mark.asyncio
async def test_over_acquire():
    limiter = AsyncLimiter(1)
    with pytest.raises(ValueError):
        await limiter.acquire(42)


async def acquire_task(limiter):
    await limiter.acquire()


async def async_contextmanager_task(limiter):
    async with limiter:
        pass


@pytest.mark.parametrize("task", [acquire_task, async_contextmanager_task])
@pytest.mark.asyncio
async def test_acquire(event_loop, task):
    current_time = 0

    def mocked_time():
        return current_time

    # capacity released every 2 seconds
    limiter = AsyncLimiter(5, 10)

    with mock.patch.object(event_loop, "time", mocked_time):
        tasks = [asyncio.ensure_future(task(limiter)) for _ in range(10)]

        pending = await wait_for_n_done(tasks, 5)
        assert len(pending) == 5

        current_time = 3  # releases capacity for one and some buffer
        assert limiter.has_capacity()

        pending = await wait_for_n_done(pending, 1)
        assert len(pending) == 4

        current_time = 7  # releases capacity for two more, plus buffer
        pending = await wait_for_n_done(pending, 2)
        assert len(pending) == 2

        current_time = 11  # releases the remainder
        pending = await wait_for_n_done(pending, 2)
        assert len(pending) == 0
