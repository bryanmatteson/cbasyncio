import asyncio

import pytest

from cbasyncio.streamz import Stream
from cbasyncio.utils import anext

from .test_utils import assert_run, event_loop

assert_run, event_loop


@pytest.mark.asyncio
async def test_split(assert_run, event_loop):
    slist = Stream.iterate(["A", "B", "C"])
    # srange = Stream.range(1, 4)

    with event_loop.assert_cleanup():
        xs = Stream.range(1, 7)
        evens, odds = xs.partition(lambda x: x % 2 == 0)
        await assert_run(evens, [2, 4, 6])
        await assert_run(odds, [1, 3, 5])

    with event_loop.assert_cleanup():
        doubled = slist + slist
        it1, it2 = doubled.split(2)
        async with it1.stream() as it1_stream, it2.stream() as it2_stream, doubled.stream() as doubled_stream:
            async for k in doubled_stream:
                a, b = await asyncio.gather(anext(it1_stream), anext(it2_stream))
                assert a == b
                assert a == k
                assert b == k

    # with event_loop.assert_cleanup():
    #     itrs = toolz.tee(self.slist * 2, n=6)

    #     for k in self.slist * 2:
    #         values = await asyncio.gather(*[anext(it) for it in itrs])
    #         for value in values:
    #             assert value == k
    #     for it in itrs:
    #         with pytest.raises(StopAsyncIteration):
    #             await anext(it)

    # with event_loop.assert_cleanup():

    #     async def gen():
    #         yield 1
    #         yield 4
    #         yield 9
    #         yield 16

    #     it1, it2 = toolz.tee(gen())

    #     for k in [1, 4, 9, 16]:
    #         a, b = await asyncio.gather(anext(it1), anext(it2))
    #         assert a == b
    #         assert a == k
    #         assert b == k
    #     for it in [it1, it2]:
    #         with pytest.raises(StopAsyncIteration):
    #             await anext(it)

    # with event_loop.assert_cleanup():

    #     async def gen():
    #         yield 1
    #         yield 4
    #         yield 9
    #         yield 16

    #     itrs = toolz.tee(gen(), n=6)

    #     for k in [1, 4, 9, 16]:
    #         values = await asyncio.gather(*[anext(it) for it in itrs])
    #         for value in values:
    #             assert value == k
    #     for it in itrs:
    #         with pytest.raises(StopAsyncIteration):
    #             await anext(it)

    # with event_loop.assert_cleanup():

    #     class MyError(Exception):
    #         pass

    #     async def gen():
    #         yield 1
    #         yield 2
    #         raise MyError

    #     async def consumer(it):
    #         result = 0
    #         async for item in it:
    #             result += item
    #         return result

    #     it1, it2 = toolz.tee(gen())

    #     values = await asyncio.gather(
    #         consumer(it1),
    #         consumer(it2),
    #         return_exceptions=True,
    #     )

    #     for value in values:
    #         assert isinstance(value, MyError)
