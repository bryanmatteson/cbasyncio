import asyncio
import operator

import pytest

from cbasyncio.streamz import stream

from .test_utils import add_resource, assert_run, event_loop

assert_run, event_loop


@pytest.mark.asyncio
async def test_aggregate(assert_run, event_loop):
    with event_loop.assert_cleanup():
        xs = stream.range(5) | add_resource.pipe(1) | stream.accumulate.pipe()
        await assert_run(xs, [0, 1, 3, 6, 10])

    with event_loop.assert_cleanup():
        xs = stream.range(2, 4) | add_resource.pipe(1) | stream.accumulate.pipe(func=operator.mul, initial=2)
        await assert_run(xs, [2, 4, 12])

    with event_loop.assert_cleanup():
        xs = stream.range(0) | add_resource.pipe(1) | stream.accumulate.pipe()
        await assert_run(xs, [])

    async def sleepmax(x: int, y: int):
        return await asyncio.sleep(1, result=max(x, y))

    with event_loop.assert_cleanup():
        xs = stream.range(3) | add_resource.pipe(1) | stream.accumulate.pipe(sleepmax)
        await assert_run(xs, [0, 1, 2])
        assert event_loop.steps == [1] * 3


@pytest.mark.asyncio
async def test_reduce(assert_run, event_loop):
    with event_loop.assert_cleanup():
        xs = stream.range(5) | add_resource.pipe(1) | stream.reduce.pipe(min)
        await assert_run(xs, [0])

    with event_loop.assert_cleanup():
        xs = stream.range(5) | add_resource.pipe(1) | stream.reduce.pipe(max)
        await assert_run(xs, [4])

    with event_loop.assert_cleanup():
        xs = stream.range(0) | add_resource.pipe(1) | stream.reduce.pipe(max)
        await assert_run(xs, [], IndexError("Index out of range"))


@pytest.mark.asyncio
async def test_list(assert_run, event_loop):
    with event_loop.assert_cleanup():
        xs = stream.range(3) | add_resource.pipe(1) | stream.list.pipe()
        # The same list object is yielded at each step
        await assert_run(xs, [[0, 1, 2], [0, 1, 2], [0, 1, 2], [0, 1, 2]])

    with event_loop.assert_cleanup():
        xs = stream.range(0) | add_resource.pipe(1) | stream.list.pipe()
        await assert_run(xs, [[]])
