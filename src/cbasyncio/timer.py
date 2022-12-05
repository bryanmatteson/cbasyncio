import asyncio
import contextlib
import enum
import functools
from typing import Any, Callable, Coroutine, Optional
from unittest import mock

from .aio import TaskGroup


class TimerDelayPolicy(enum.Enum):
    DEFAULT = 0
    CANCEL = 1


def create_timer(
    cb: Callable[[float], Coroutine[Any, Any, None]],
    interval: float,
    delay_policy: TimerDelayPolicy = TimerDelayPolicy.DEFAULT,
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> asyncio.Task:
    if loop is None:
        loop = asyncio.get_running_loop()

    async def _timer():
        fired_tasks = []
        try:
            async with TaskGroup() as task_group:
                while True:
                    if delay_policy == TimerDelayPolicy.CANCEL:
                        for t in fired_tasks:
                            if not t.done():
                                t.cancel()
                        await asyncio.gather(*fired_tasks, return_exceptions=True)
                        fired_tasks.clear()
                    else:
                        fired_tasks[:] = [t for t in fired_tasks if not t.done()]
                    t = task_group.start_soon(cb, interval)
                    fired_tasks.append(t)
                    await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass
        finally:
            await asyncio.sleep(0)

    return loop.create_task(_timer())


class VirtualClock:
    def __init__(self) -> None:
        self.vtime = 0.0

    def virtual_time(self) -> float:
        return self.vtime

    def _virtual_select(self, orig_select: Callable[[float], Any], timeout: Optional[float]) -> Any:
        if timeout is not None:
            self.vtime += timeout
        return orig_select(0)

    @contextlib.contextmanager
    def patch_loop(self):
        loop = asyncio.get_running_loop()
        selector = getattr(loop, "_selector")
        with mock.patch.object(selector, "select", new=functools.partial(self._virtual_select, selector.select)):
            with mock.patch.object(loop, "time", new=self.virtual_time):
                yield
