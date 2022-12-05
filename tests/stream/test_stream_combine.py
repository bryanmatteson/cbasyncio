import pytest

from cbasyncio.streamz import stream

from .test_utils import assert_run, event_loop

assert_run, event_loop


@pytest.mark.asyncio
async def test_concatmap(assert_run, event_loop):
    def pipefn(x: int):
        return stream.range(x, x + 2, interval=5)

    # Concurrent run
    with event_loop.assert_cleanup():
        xs = stream.range(0, 6, 2, interval=1)
        ys = xs | stream.concatmap.pipe(pipefn)
        await assert_run(ys, [0, 1, 2, 3, 4, 5])
        assert event_loop.steps == [1, 1, 3, 5, 5]

    # Sequential run
    with event_loop.assert_cleanup():
        xs = stream.range(0, 6, 2, interval=1)
        ys = xs | stream.concatmap.pipe(pipefn, task_limit=1)
        await assert_run(ys, [0, 1, 2, 3, 4, 5])
        assert event_loop.steps == [5, 1, 5, 1, 5]

    # Limited run
    with event_loop.assert_cleanup():
        xs = stream.range(0, 6, 2, interval=1)
        ys = xs | stream.concatmap.pipe(pipefn, task_limit=2)
        await assert_run(ys, [0, 1, 2, 3, 4, 5])
        assert event_loop.steps == [1, 4, 1, 4, 5]

    # Make sure item arrive as soon as possible
    with event_loop.assert_cleanup():
        xs = stream.just(2)

        def pipefn2(x: int):
            return stream.range(x, x + 4, interval=1)

        ys = xs | stream.concatmap.pipe(pipefn2)
        zs = ys | stream.timeout.pipe(2)  # Sould NOT raise
        await assert_run(zs, [2, 3, 4, 5])
        assert event_loop.steps == [1, 1, 1]

    # An exception might get discarded if the result can be produced before the
    # processing of the exception is required
    with event_loop.assert_cleanup():
        xs = stream.iterate([True, False])

        def pipefn3(x: int):
            return stream.range(0, 3, interval=1) if x else stream.throw(ZeroDivisionError)

        ys = xs | stream.concatmap.pipe(pipefn3)
        zs = ys | stream.take.pipe(3)
        await assert_run(zs, [0, 1, 2])
        assert event_loop.steps == [1, 1]


@pytest.mark.asyncio
async def test_flatmap(assert_run, event_loop):
    def pipefn(x: int):
        return stream.range(x, x + 2, interval=5)

    # Concurrent run
    with event_loop.assert_cleanup():
        xs = stream.range(0, 6, 2, interval=1)
        ys = xs | stream.flatmap.pipe(pipefn)
        await assert_run(ys, [0, 2, 4, 1, 3, 5])
        assert event_loop.steps == [1, 1, 3, 1, 1]

    # Sequential run
    with event_loop.assert_cleanup():
        xs = stream.range(0, 6, 2, interval=1)
        ys = xs | stream.flatmap.pipe(pipefn, task_limit=1)
        await assert_run(ys, [0, 1, 2, 3, 4, 5])
        assert event_loop.steps == [5, 1, 5, 1, 5]

    # Limited run
    with event_loop.assert_cleanup():
        xs = stream.range(0, 6, 2, interval=1)
        ys = xs | stream.flatmap.pipe(pipefn, task_limit=2)
        await assert_run(ys, [0, 2, 1, 3, 4, 5])
        assert event_loop.steps == [1, 4, 1, 5]


@pytest.mark.asyncio
async def test_switchmap(assert_run, event_loop):
    def pipefn(x: int):
        return stream.range(x, x + 5, interval=1)

    def pipefn2(x: int):
        return stream.range(x, x + 2, interval=2)

    with event_loop.assert_cleanup():
        xs = stream.range(0, 30, 10, interval=3)
        ys = xs | stream.switchmap.pipe(pipefn)
        await assert_run(ys, [0, 1, 2, 10, 11, 12, 20, 21, 22, 23, 24])
        assert event_loop.steps == [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

    # Test cleanup procedure
    with event_loop.assert_cleanup():
        xs = stream.range(0, 5, interval=1)
        ys = xs | stream.switchmap.pipe(pipefn2)
        await assert_run(ys[:3], [0, 1, 2])
        assert event_loop.steps == [1, 1]
