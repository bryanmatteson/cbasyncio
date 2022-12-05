import asyncio

import pytest

from cbasyncio.streamz import stream

from .test_utils import add_resource, assert_run, event_loop

assert_run, event_loop


@pytest.mark.asyncio
async def test_starmap(assert_run, event_loop):
    with event_loop.assert_cleanup():
        xs = stream.range(5)
        ys = stream.range(5)
        zs = xs | stream.zip.pipe(ys) | stream.starmap.pipe(lambda x, y: x + y)
        expected = [x * 2 for x in range(5)]
        await assert_run(zs, expected)

    with event_loop.assert_cleanup():
        xs = stream.range(1, 4)
        ys = stream.range(1, 4)
        zs = xs | stream.zip.pipe(ys) | stream.starmap.pipe(asyncio.sleep)
        await assert_run(zs, [1, 2, 3])
        assert event_loop.steps == [1, 1, 1]

    with event_loop.assert_cleanup():
        xs = stream.range(1, 4)
        ys = stream.range(1, 4)
        zs = xs | stream.zip.pipe(ys) | stream.starmap.pipe(asyncio.sleep, task_limit=1)
        await assert_run(zs, [1, 2, 3])
        assert event_loop.steps == [1, 2, 3]


@pytest.mark.asyncio
async def test_chunks(assert_run, event_loop):
    with event_loop.assert_cleanup():
        xs = stream.range(3, interval=1) | stream.chunks.pipe(3)
        await assert_run(xs, [[0, 1, 2]])

    with event_loop.assert_cleanup():
        xs = stream.range(4, interval=1) | stream.chunks.pipe(3)
        await assert_run(xs, [[0, 1, 2], [3]])

    with event_loop.assert_cleanup():
        xs = stream.range(5, interval=1) | stream.chunks.pipe(3)
        await assert_run(xs, [[0, 1, 2], [3, 4]])

    with event_loop.assert_cleanup():
        xs = stream.count(interval=1) | add_resource.pipe(1) | stream.chunks.pipe(3)
        await assert_run(xs[:1], [[0, 1, 2]])
