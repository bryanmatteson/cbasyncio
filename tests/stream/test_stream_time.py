import asyncio

import pytest

from cbasyncio.streamz import stream

from .test_utils import assert_run, event_loop

assert_run, event_loop


@pytest.mark.asyncio
async def test_timeout(assert_run, event_loop):
    with event_loop.assert_cleanup():
        xs = stream.range(3) | stream.timeout.pipe(5)
        await assert_run(xs, [0, 1, 2])
        assert event_loop.steps == []

    with event_loop.assert_cleanup():
        xs = stream.range(3) + stream.never()
        ys = xs | stream.timeout.pipe(1)
        await assert_run(ys, [0, 1, 2], asyncio.TimeoutError())
        assert event_loop.steps == [1]
