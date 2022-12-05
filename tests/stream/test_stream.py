import asyncio
from typing import AsyncIterator

import pytest

from cbasyncio.streamz import toolz
from cbasyncio.utils import anext


class TestAioItertools:
    slist = ["A", "B", "C"]
    srange = range(1, 4)

    @pytest.mark.asyncio
    async def test_chain_lists(self):
        it = toolz.chain(self.slist, self.srange)
        for k in ["A", "B", "C", 1, 2, 3]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_chain_list_gens(self):
        async def gen():
            for k in range(2, 9, 2):
                yield k

        it = toolz.chain(self.slist, gen())
        for k in ["A", "B", "C", 2, 4, 6, 8]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_chain_from_iterable(self):
        async def gen():
            for k in range(2, 9, 2):
                yield k

        it = toolz.chain(self.slist, gen())
        for k in ["A", "B", "C", 2, 4, 6, 8]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_chain_from_iterable_parameter_expansion_gen(self):
        async def gen():
            for k in range(2, 9, 2):
                yield k

        async def parameters_gen():
            yield self.slist
            yield gen()

        it = toolz.chain_from_iterable(parameters_gen())
        for k in ["A", "B", "C", 2, 4, 6, 8]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_count_bare(self):
        it = toolz.count()
        for k in [0, 1, 2, 3]:
            assert await anext(it) == k

    @pytest.mark.asyncio
    async def test_count_start(self):
        it = toolz.count(42)
        for k in [42, 43, 44, 45]:
            assert await anext(it) == k

    @pytest.mark.asyncio
    async def test_count_start_step(self):
        it = toolz.count(42, 3)
        for k in [42, 45, 48, 51]:
            assert await anext(it) == k

    @pytest.mark.asyncio
    async def test_count_negative(self):
        it = toolz.count(step=-2)
        for k in [0, -2, -4, -6]:
            assert await anext(it) == k

    @pytest.mark.asyncio
    async def test_dropwhile_empty(self):
        def pred(x):
            return x < 2

        result = await toolz.list(toolz.dropwhile(pred, []))
        assert result == []

    @pytest.mark.asyncio
    async def test_dropwhile_function_list(self):
        def pred(x):
            return x < 2

        it = toolz.dropwhile(pred, self.srange)
        for k in [2, 3]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_dropwhile_function_gen(self):
        def pred(x):
            return x < 2

        async def gen():
            yield 1
            yield 2
            yield 42

        it = toolz.dropwhile(pred, gen())
        for k in [2, 42]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_dropwhile_coroutine_list(self):
        async def pred(x):
            return x < 2

        it = toolz.dropwhile(pred, self.srange)
        for k in [2, 3]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_dropwhile_coroutine_gen(self):
        async def pred(x):
            return x < 2

        async def gen():
            yield 1
            yield 2
            yield 42

        it = toolz.dropwhile(pred, gen())
        for k in [2, 42]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_filterfalse_function_list(self):
        def pred(x):
            return x % 2 == 0

        it = toolz.filterfalse(pred, self.srange)
        for k in [1, 3]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_filterfalse_coroutine_list(self):
        async def pred(x):
            return x % 2 == 0

        it = toolz.filterfalse(pred, self.srange)
        async for k in toolz.from_iterable([1, 3]):
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_groupby_list(self):
        data = "aaabba"

        it = toolz.groupby(data)
        for k in [("a", ["a", "a", "a"]), ("b", ["b", "b"]), ("a", ["a"])]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_groupby_list_key(self):
        data = "aAabBA"

        it = toolz.groupby(data, key_fn=str.lower)
        for k in [("a", ["a", "A", "a"]), ("b", ["b", "B"]), ("a", ["A"])]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_groupby_gen(self):
        async def gen():
            for c in "aaabba":
                yield c

        it = toolz.groupby(gen())
        for k in [("a", ["a", "a", "a"]), ("b", ["b", "b"]), ("a", ["a"])]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_groupby_gen_key(self):
        async def gen():
            for c in "aAabBA":
                yield c

        it = toolz.groupby(gen(), key_fn=str.lower)
        for k in [("a", ["a", "A", "a"]), ("b", ["b", "B"]), ("a", ["A"])]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_groupby_empty(self):
        async def gen():
            for _ in range(0):
                yield  # Force generator with no actual iteration

        async for _ in toolz.groupby(gen()):
            assert False, "No iteration should have happened"

    @pytest.mark.asyncio
    async def test_islice_stop_zero(self):
        values = []
        async for value in toolz.slice(range(5), 0):
            values.append(value)
        assert not values

    @pytest.mark.asyncio
    async def test_islice_range_stop(self):
        it = toolz.slice(self.srange, 2)
        for k in [1, 2]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_islice_range_start_step(self):
        it = toolz.slice(self.srange, 0, None, 2)
        for k in [1, 3]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_islice_range_start_stop(self):
        it = toolz.slice(self.srange, 1, 3)
        for k in [2, 3]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_islice_range_start_stop_step(self):
        it = toolz.slice(self.srange, 1, 3, 2)
        for k in [2]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_islice_gen_stop(self):
        async def gen():
            yield 1
            yield 2
            yield 3
            yield 4

        gen_it = gen()
        it = toolz.slice(toolz.preserve(gen_it), 2)
        for k in [1, 2]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)
        assert await toolz.list(gen_it) == [3, 4]

    @pytest.mark.asyncio
    async def test_islice_gen_start_step(self):
        async def gen():
            yield 1
            yield 2
            yield 3
            yield 4

        it = toolz.slice(gen(), 1, None, 2)
        for k in [2, 4]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_islice_gen_start_stop(self):
        async def gen():
            yield 1
            yield 2
            yield 3
            yield 4

        it = toolz.slice(gen(), 1, 3)
        for k in [2, 3]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_islice_gen_start_stop_step(self):
        async def gen():
            yield 1
            yield 2
            yield 3
            yield 4

        gen_it = gen()
        it = toolz.slice(toolz.preserve(gen_it), 1, 3, 2)
        for k in [2]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)
        assert await toolz.list(gen_it) == [4]

    @pytest.mark.asyncio
    async def test_repeat(self):
        it = toolz.repeat(42)
        for k in [42] * 10:
            assert await anext(it) == k

    @pytest.mark.asyncio
    async def test_repeat_limit(self):
        it = toolz.repeat(42, 5)
        for k in [42] * 5:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_starmap_function_list(self):
        data = [self.slist[:2], self.slist[1:], self.slist]

        def concat(*args):
            return "".join(args)

        it = toolz.starmap(concat, data)
        for k in ["AB", "BC", "ABC"]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_starmap_function_gen(self):
        def gen():
            yield self.slist[:2]
            yield self.slist[1:]
            yield self.slist

        def concat(*args):
            return "".join(args)

        it = toolz.starmap(concat, gen())
        for k in ["AB", "BC", "ABC"]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_starmap_coroutine_list(self):
        data = [self.slist[:2], self.slist[1:], self.slist]

        async def concat(*args):
            return "".join(args)

        it = toolz.starmap(concat, data)
        for k in ["AB", "BC", "ABC"]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_starmap_coroutine_gen(self):
        async def gen():
            yield self.slist[:2]
            yield self.slist[1:]
            yield self.slist

        async def concat(*args):
            return "".join(args)

        it = toolz.starmap(concat, gen())
        for k in ["AB", "BC", "ABC"]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_takewhile_empty(self):
        def pred(x):
            return x < 3

        values = await toolz.list(toolz.takewhile(pred, []))
        assert values == []

    @pytest.mark.asyncio
    async def test_takewhile_function_list(self):
        def pred(x):
            return x < 3

        it = toolz.takewhile(pred, self.srange)
        for k in [1, 2]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_takewhile_function_gen(self):
        async def gen():
            yield 1
            yield 2
            yield 3

        def pred(x):
            return x < 3

        it = toolz.takewhile(pred, gen())
        for k in [1, 2]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_takewhile_coroutine_list(self):
        async def pred(x):
            return x < 3

        it = toolz.takewhile(pred, self.srange)
        for k in [1, 2]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_takewhile_coroutine_gen(self):
        def gen():
            yield 1
            yield 2
            yield 3

        async def pred(x):
            return x < 3

        it = toolz.takewhile(pred, gen())
        for k in [1, 2]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_tee_list_two(self):
        it1, it2 = toolz.tee(self.slist * 2)

        for k in self.slist * 2:
            a, b = await asyncio.gather(anext(it1), anext(it2))
            assert a == b
            assert a == k
            assert b == k
        for it in [it1, it2]:
            with pytest.raises(StopAsyncIteration):
                await anext(it)

    @pytest.mark.asyncio
    async def test_tee_list_six(self):
        itrs = toolz.tee(self.slist * 2, n=6)

        for k in self.slist * 2:
            values = await asyncio.gather(*[anext(it) for it in itrs])
            for value in values:
                assert value == k
        for it in itrs:
            with pytest.raises(StopAsyncIteration):
                await anext(it)

    @pytest.mark.asyncio
    async def test_tee_gen_two(self):
        async def gen():
            yield 1
            yield 4
            yield 9
            yield 16

        it1, it2 = toolz.tee(gen())

        for k in [1, 4, 9, 16]:
            a, b = await asyncio.gather(anext(it1), anext(it2))
            assert a == b
            assert a == k
            assert b == k
        for it in [it1, it2]:
            with pytest.raises(StopAsyncIteration):
                await anext(it)

    @pytest.mark.asyncio
    async def test_tee_gen_six(self):
        async def gen():
            yield 1
            yield 4
            yield 9
            yield 16

        itrs = toolz.tee(gen(), n=6)

        for k in [1, 4, 9, 16]:
            values = await asyncio.gather(*[anext(it) for it in itrs])
            for value in values:
                assert value == k
        for it in itrs:
            with pytest.raises(StopAsyncIteration):
                await anext(it)

    @pytest.mark.asyncio
    async def test_tee_propagate_exception(self):
        class MyError(Exception):
            pass

        async def gen():
            yield 1
            yield 2
            raise MyError

        async def consumer(it):
            result = 0
            async for item in it:
                result += item
            return result

        it1, it2 = toolz.tee(gen())

        values = await asyncio.gather(
            consumer(it1),
            consumer(it2),
            return_exceptions=True,
        )

        for value in values:
            assert isinstance(value, MyError)

    @pytest.mark.asyncio
    async def test_ziplongest_range(self):
        a = range(3)
        b = range(5)

        it = toolz.ziplongest(a, b)

        for k in [(0, 0), (1, 1), (2, 2), (None, 3), (None, 4)]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_ziplongest_fillvalue(self):
        async def gen() -> AsyncIterator[int]:
            yield 1
            yield 4
            yield 9
            yield 16

        a = gen()
        b = range(5)

        it = toolz.ziplongest(a, b, fillvalue=42)

        for k in [(1, 0), (4, 1), (9, 2), (16, 3), (42, 4)]:
            assert await anext(it) == k
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test_ziplongest_exception(self):
        async def gen() -> AsyncIterator[int]:
            yield 1
            yield 2
            raise Exception("fake error")

        a = gen()
        b = toolz.repeat(5)

        it = toolz.ziplongest(a, b)

        for k in [(1, 5), (2, 5)]:
            assert await anext(it) == k
        with pytest.raises(Exception, match="fake error"):
            await anext(it)

    @pytest.mark.asyncio
    async def test_partition(self):
        async def gen() -> AsyncIterator[int]:
            yield 1
            yield 2
            yield 3
            yield 4
            yield 5
            yield 6

        evens, odds = toolz.partition(gen(), lambda x: x % 2 == 0)
        assert await toolz.list(odds) == [1, 3, 5]
        assert await toolz.list(evens) == [2, 4, 6]
