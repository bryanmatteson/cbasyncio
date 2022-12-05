from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Dict, Optional, Type


class RateLimiter:
    def __init__(self, max_per_second: float) -> None:
        self.period = 1 / max_per_second
        self.max_per_period = 1
        self.next_start_time = 0.0

    @property
    def task_delta(self) -> float:
        return self.period / self.max_per_period

    async def wait_begin(self) -> None:
        while True:
            now = asyncio.get_running_loop().time()
            next_start_time = max(self.next_start_time, now)
            time_until_start = next_start_time - now
            threshold = self.period - self.task_delta
            if time_until_start <= threshold:
                break
            await asyncio.sleep(max(0, time_until_start - threshold))

    async def notify_task_start(self) -> None:
        now = asyncio.get_running_loop().time()
        self.next_start_time = max(self.next_start_time, now) + self.task_delta

    async def notify_task_done(self) -> None:
        pass

    async def __aenter__(self):
        await self.wait_begin()
        await self.notify_task_start()
        return self

    async def __aexit__(self, *args):
        await self.notify_task_done()


class AsyncLimiter:
    max_rate: float
    time_period: float

    def __init__(self, max_rate: float, time_period: float = 60) -> None:
        self.max_rate = max_rate
        self.time_period = time_period
        self._rate_per_sec = max_rate / time_period
        self._level = 0.0
        self._last_check = 0.0
        self._waiters: Dict[asyncio.Task, asyncio.Future] = {}

    def _leak(self) -> None:
        loop = asyncio.get_running_loop()
        if self._level:
            elapsed = loop.time() - self._last_check
            decrement = elapsed * self._rate_per_sec
            self._level = max(self._level - decrement, 0)
        self._last_check = loop.time()

    def has_capacity(self, amount: float = 1) -> bool:
        self._leak()
        requested = self._level + amount

        if requested < self.max_rate:
            for fut in self._waiters.values():
                if not fut.done():
                    fut.set_result(True)
                    break
        return self._level + amount <= self.max_rate

    async def acquire(self, amount: float = 1) -> None:
        if amount > self.max_rate:
            raise ValueError("Can't acquire more than the maximum capacity")

        loop = asyncio.get_running_loop()
        task = asyncio.current_task(loop)
        assert task is not None
        while not self.has_capacity(amount):
            fut = loop.create_future()
            self._waiters[task] = fut
            try:
                await asyncio.wait_for(asyncio.shield(fut), 1 / self._rate_per_sec * amount)
            except asyncio.TimeoutError:
                pass
            fut.cancel()
        self._waiters.pop(task, None)
        self._level += amount

        return None

    async def __aenter__(self) -> None:
        await self.acquire()
        return None

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb: Optional[TracebackType],
    ) -> None:
        return None
