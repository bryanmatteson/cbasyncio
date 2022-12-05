import pytest

from cbasyncio.streamz import stream, streamcontext

from .test_utils import add_resource, assert_run, event_loop

assert_run, event_loop


@pytest.mark.asyncio
async def test_streamcontext(event_loop):

    with event_loop.assert_cleanup():
        xs = stream.range(3) | add_resource.pipe(1)
        async with streamcontext(xs) as streamer:
            it = iter(range(3))
            async for item in streamer:
                assert item == next(it)
        assert event_loop.steps == [1]

    with event_loop.assert_cleanup():
        xs = stream.range(5) | add_resource.pipe(1)
        async with xs.stream() as streamer:
            it = iter(range(5))
            async for item in streamer:
                assert item == next(it)
        assert event_loop.steps == [1]


@pytest.mark.asyncio
async def test_error_on_sync_iteration(event_loop):
    xs = stream.range(3)

    # Stream raises a TypeError
    with pytest.raises(TypeError):
        for x in xs:
            assert False

    # Streamer raises a TypeError
    async with xs.stream() as streamer:
        with pytest.raises(TypeError):
            for x in streamer:
                assert False


@pytest.mark.asyncio
async def test_error_on_entering_a_stream(event_loop):
    xs = stream.range(3)

    # Stream raises a TypeError
    with pytest.raises(TypeError) as ctx:
        async with xs:
            assert False

    assert "Use the `stream` method" in str(ctx.value)
