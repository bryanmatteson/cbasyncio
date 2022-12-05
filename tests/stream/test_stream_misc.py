import asyncio
import io

import pytest

from cbasyncio.streamz import stream

from .test_utils import add_resource, assert_run, event_loop

assert_run, event_loop


@pytest.mark.asyncio
async def test_action(assert_run, event_loop):
    with event_loop.assert_cleanup():
        lst = []
        xs = stream.range(3) | add_resource.pipe(1) | stream.action.pipe(lst.append)
        await assert_run(xs, [0, 1, 2])
        assert lst == [0, 1, 2]

    with event_loop.assert_cleanup():
        queue = asyncio.Queue()
        xs = stream.range(3) | add_resource.pipe(1) | stream.action.pipe(queue.put)
        await assert_run(xs, [0, 1, 2])
        assert queue.get_nowait() == 0
        assert queue.get_nowait() == 1
        assert queue.get_nowait() == 2


@pytest.mark.asyncio
async def test_print(assert_run, event_loop):
    with event_loop.assert_cleanup():
        f = io.StringIO()
        xs = stream.range(3) | add_resource.pipe(1) | stream.print.pipe(file=f)
        await assert_run(xs, [0, 1, 2])
        assert f.getvalue() == "0\n1\n2\n"

    with event_loop.assert_cleanup():
        f = io.StringIO()
        xs = stream.range(3) | add_resource.pipe(1) | stream.print.pipe("{:.1f}", end="|", file=f)
        await assert_run(xs, [0, 1, 2])
        assert f.getvalue() == "0.0|1.0|2.0|"


@pytest.mark.asyncio
async def test_split(assert_run, event_loop):
    with event_loop.assert_cleanup():
        st1, st2, st3, st4 = stream.range(3).split(4)
        await assert_run(st1, [0, 1, 2])
        await assert_run(st2, [0, 1, 2])
        await assert_run(st3, [0, 1, 2])
        await assert_run(st4, [0, 1, 2])
