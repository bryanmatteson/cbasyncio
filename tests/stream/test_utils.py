import asyncio
from contextlib import contextmanager
from typing import Any, AsyncIterable, Iterable, List, Optional, Union
from unittest.mock import Mock

import pytest

from cbasyncio.streamz import StreamEmpty, operator, streamcontext


@operator(pipable=True)
async def add_resource(source: Union[Iterable[Any], AsyncIterable[Any]], cleanup_time: float):
    try:
        loop = asyncio.get_event_loop()
        loop.open_resources += 1  # type: ignore
        loop.resources += 1  # type: ignore
        async with streamcontext(source) as streamer:
            async for item in streamer:
                yield item
    finally:
        try:
            await asyncio.sleep(cleanup_time)
        finally:
            loop.open_resources -= 1  # type: ignore


def compare_exceptions(exc1: Any, exc2: Any) -> bool:
    return exc1 == exc2 or exc1.__class__ == exc2.__class__ and exc1.args == exc2.args


async def assert_aiter(source: AsyncIterable[Any], values: List[Any], exception: Optional[BaseException] = None):
    """Check the results of a stream using a streamcontext."""
    results = []
    exception_type = type(exception) if exception else ()
    try:
        async with streamcontext(source) as streamer:
            async for item in streamer:
                results.append(item)
    except exception_type as exc:
        assert compare_exceptions(exc, exception)
    else:
        assert exception is None
    assert results == values


async def assert_await(source, values, exception=None):
    """Check the results of a stream using by awaiting it."""
    exception_type = type(exception) if exception else Exception
    try:
        result = await source
    except StreamEmpty:
        assert values == []
        assert exception is None
    except exception_type as exc:
        assert compare_exceptions(exc, exception)
    else:
        assert result == values[-1]
        assert exception is None


@pytest.fixture(params=[assert_aiter, assert_await], ids=["aiter", "await"])
def assert_run(request):
    return request.param


@pytest.fixture
def event_loop():
    class TimeTrackingTestLoop(asyncio.BaseEventLoop):

        stuck_threshold = 100

        def __init__(self):
            super().__init__()
            self._time = 0
            self._timers = []
            self._selector = Mock()
            self.clear()

        # Loop internals

        def _run_once(self):
            super()._run_once()  # type: ignore
            # Update internals
            self.busy_count += 1
            self._timers = sorted(when for when in self._timers if when > loop.time())
            # Time advance
            if self.time_to_go:
                when = self._timers.pop(0)
                step = when - loop.time()
                self.steps.append(step)
                self.advance_time(step)
                self.busy_count = 0

        def _process_events(self, event_list):
            return

        def _write_to_self(self):
            return

        # Time management

        def time(self):
            return self._time

        def advance_time(self, advance):
            if advance:
                self._time += advance

        def call_at(self, when, callback, *args, **kwargs):
            self._timers.append(when)
            return super().call_at(when, callback, *args, **kwargs)

        @property
        def stuck(self):
            return self.busy_count > self.stuck_threshold

        @property
        def time_to_go(self):
            return self._timers and (self.stuck or not self._ready)  # type: ignore

        # Resource management

        def clear(self):
            self.steps = []
            self.open_resources = 0
            self.resources = 0
            self.busy_count = 0

        @contextmanager
        def assert_cleanup(self):
            self.clear()
            yield self
            assert self.open_resources == 0
            self.clear()

    loop = TimeTrackingTestLoop()
    asyncio.set_event_loop(loop)
    with loop.assert_cleanup():
        yield loop
    loop.close()
