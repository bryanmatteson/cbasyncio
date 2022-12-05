import asyncio
import builtins
import collections
import inspect
import itertools
from contextlib import AsyncExitStack
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Deque,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    SupportsIndex,
    Tuple,
    Type,
    Union,
    cast,
    overload,
)

from typing_extensions import Unpack

from cbasyncio.types import Sentinel, Unset

from .._types import ADD, T0, T1, T2, T3, T4, T5, A, AnyIterable, AnyStop, KeyFunction, P, Predicate, R, T
from ..context import AsyncIteratorContext, aitercontext
from cbasyncio.types import NoLock
from ..utils import anext, anyiter, identity, is_coro_function, maybe_await

_acc_sentinel = Sentinel("<no default>")


async def add(x: ADD, y: ADD) -> ADD:
    return x + y


async def from_iterable(it: Iterable[T]) -> AsyncIterator[T]:
    for item in it:
        await asyncio.sleep(0)
        yield item


def from_async_iterable(ait: AsyncIterable[T]) -> AsyncIteratorContext[T]:
    return aitercontext(ait)


def iterate(it: AnyIterable[T]) -> AsyncIterator[T]:
    if isinstance(it, AsyncIterable):
        return from_async_iterable(it)

    if isinstance(it, Iterable):
        return from_iterable(it)

    raise TypeError(f"{type(it).__name__!r} object is not (async) iterable")


async def preserve(ait: AsyncIterable[T]) -> AsyncIterator[T]:
    async for item in ait:
        yield item


async def just(value: Union[Awaitable[T], T]) -> AsyncIterator[T]:
    if inspect.isawaitable(value):
        yield await value
    else:
        yield cast(T, value)


async def call(
    func: Callable[P, Union[Coroutine[Any, Any, T], T]], *args: P.args, **kwargs: P.kwargs
) -> AsyncIterator[T]:
    if is_coro_function(func):
        yield await func(*args, **kwargs)
    else:
        yield cast(T, func(*args, **kwargs))


async def throw(exc: Union[Type[BaseException], BaseException]) -> AsyncIterator[None]:
    if False:
        yield
    raise exc


async def empty() -> AsyncIterator[None]:
    if False:
        yield


async def never() -> AsyncIterator[None]:
    if False:
        yield
    future = asyncio.Future()
    try:
        await future
    finally:
        future.cancel()


def repeat(value: T, times: Optional[int] = None, *, interval: float = 0.0) -> AsyncIterator[T]:
    args = () if times is None else (times,)
    agen = from_iterable(itertools.repeat(value, *args))
    return spaceout(agen, interval) if interval else agen


def range(*args: SupportsIndex, interval: float = 0.0) -> AsyncIterator[int]:
    agen = from_iterable(builtins.range(*args))
    if not interval:
        return agen
    return spaceout(agen, interval)


def count(start: int = 0, step: int = 1, *, interval: float = 0.0) -> AsyncIterator[int]:
    agen = from_iterable(itertools.count(start, step))
    return spaceout(agen, interval) if interval else agen


async def base_combine(
    source: AnyIterable[AnyIterable[R]],
    switch: bool = False,
    ordered: bool = False,
    task_limit: Optional[int] = None,
) -> AsyncIterator[R]:
    from .stream import StreamerManager

    if task_limit is not None and task_limit <= 0:
        raise ValueError("The task limit must be None or greater than 0")

    it = anyiter(source)
    async with StreamerManager() as manager:
        main_streamer = await manager.enter_and_create_task(it)
        while manager.tasks:
            substreamers = manager.streamers[1:]
            mainstreamers = [main_streamer] if main_streamer in manager.tasks else []
            if switch:
                filters = mainstreamers + substreamers
            elif ordered:
                filters = substreamers[:1] + mainstreamers
            else:
                filters = substreamers + mainstreamers

            streamer, task = await manager.wait_single_event(filters)

            try:
                result = task.result()
            except StopAsyncIteration:

                if streamer is main_streamer:
                    main_streamer = None
                else:
                    await manager.clean_streamer(streamer)
                    if main_streamer is not None and main_streamer not in manager.tasks:
                        manager.create_task(main_streamer)
            else:
                if switch and streamer is main_streamer:
                    await manager.clean_streamers(substreamers)

                if streamer is main_streamer:
                    await manager.enter_and_create_task(result)

                    if task_limit is None or task_limit > len(manager.tasks):
                        manager.create_task(streamer)

                else:
                    yield result
                    manager.create_task(streamer)


def concat(source: AnyIterable[AnyIterable[R]], task_limit: Optional[int] = None) -> AsyncIterator[R]:
    return base_combine(source, task_limit=task_limit, switch=False, ordered=True)


def flatten(source: AnyIterable[AnyIterable[R]], task_limit: Optional[int] = None) -> AsyncIterator[R]:
    return base_combine(source, task_limit=task_limit, switch=False, ordered=False)


def switch(source: AnyIterable[AnyIterable[R]]) -> AsyncIterator[R]:
    return base_combine(source, switch=True)


@overload
def concatmap(
    func: Callable[[T], AnyIterable[R]],
    source: AnyIterable[T],
    /,
    *,
    task_limit: Optional[int] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def concatmap(
    func: Callable[[T0, T1], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    /,
    *,
    task_limit: Optional[int] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def concatmap(
    func: Callable[[T0, T1, T2], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    /,
    *,
    task_limit: Optional[int] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def concatmap(
    func: Callable[[T0, T1, T2, T3], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    /,
    *,
    task_limit: Optional[int] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def concatmap(
    func: Callable[[T0, T1, T2, T3, T4], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    /,
    *,
    task_limit: Optional[int] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def concatmap(
    func: Callable[[T0, T1, T2, T3, T4, T5, Unpack[Tuple[Any, ...]]], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    source5: AnyIterable[T5],
    /,
    *sources: AnyIterable[Any],
    task_limit: Optional[int] = ...,
) -> AsyncIterator[R]:
    ...


def concatmap(
    func: Callable[..., AnyIterable[Any]],
    *sources: AnyIterable[Any],
    task_limit: Optional[int] = None,
) -> AsyncIterator[Any]:
    return concat(smap(func, *sources), task_limit=task_limit)


@overload
def flatmap(
    func: Callable[[T], AnyIterable[R]],
    source: AnyIterable[T],
    /,
    *,
    task_limit: Optional[int] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def flatmap(
    func: Callable[[T0, T1], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    /,
    *,
    task_limit: Optional[int] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def flatmap(
    func: Callable[[T0, T1, T2], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    /,
    *,
    task_limit: Optional[int] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def flatmap(
    func: Callable[[T0, T1, T2, T3], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    /,
    *,
    task_limit: Optional[int] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def flatmap(
    func: Callable[[T0, T1, T2, T3, T4], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    /,
    *,
    task_limit: Optional[int] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def flatmap(
    func: Callable[[T0, T1, T2, T3, T4, T5, Unpack[Tuple[Any, ...]]], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    source5: AnyIterable[T5],
    /,
    *sources: AnyIterable[Any],
    task_limit: Optional[int] = ...,
) -> AsyncIterator[R]:
    ...


def flatmap(
    func: Callable[..., AnyIterable[Any]],
    *sources: AnyIterable[Any],
    task_limit: Optional[int] = None,
) -> AsyncIterator[Any]:
    return flatten(smap(func, *sources), task_limit=task_limit)


@overload
def switchmap(func: Callable[[T], AnyIterable[R]], source: AnyIterable[T], /) -> AsyncIterator[R]:
    ...


@overload
def switchmap(
    func: Callable[[T0, T1], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    /,
) -> AsyncIterator[R]:
    ...


@overload
def switchmap(
    func: Callable[[T0, T1, T2], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    /,
) -> AsyncIterator[R]:
    ...


@overload
def switchmap(
    func: Callable[[T0, T1, T2, T3], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    /,
) -> AsyncIterator[R]:
    ...


@overload
def switchmap(
    func: Callable[[T0, T1, T2, T3, T4], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    /,
) -> AsyncIterator[R]:
    ...


@overload
def switchmap(
    func: Callable[[T0, T1, T2, T3, T4, T5, Unpack[Tuple[Any, ...]]], AnyIterable[R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    source5: AnyIterable[T5],
    /,
    *sources: AnyIterable[Any],
) -> AsyncIterator[R]:
    ...


def switchmap(func: Callable[..., AnyIterable[Any]], *sources: AnyIterable[Any]) -> AsyncIterator[Any]:
    return switch(smap(func, *sources))


@overload
def accumulate(source: AnyIterable[ADD]) -> AsyncIterator[ADD]:
    ...


@overload
def accumulate(source: AnyIterable[ADD], *, initial: ADD) -> AsyncIterator[ADD]:
    ...


@overload
def accumulate(
    source: AnyIterable[T],
    func: Callable[[Union[T, R], T], Union[Coroutine[Any, Any, R], R]],
) -> AsyncIterator[R]:
    ...


@overload
def accumulate(
    source: AnyIterable[T],
    func: Callable[[Union[A, R], T], Union[Coroutine[Any, Any, R], R]],
    *,
    initial: A,
) -> AsyncIterator[R]:
    ...


async def accumulate(
    source: AnyIterable[Any],
    func: Callable[[Any, Any], Union[Coroutine[Any, Any, Any], Any]] = add,
    *,
    initial: Any = Unset,
) -> AsyncIterator[Any]:
    async with aitercontext(source) as streamer:
        if initial is Unset:
            try:
                value = await anext(streamer)
            except StopAsyncIteration:
                return
        else:
            value = initial

        yield cast(R, value)
        if is_coro_function(func):
            async for item in streamer:
                value = await func(value, item)
                yield value
        else:
            async for item in streamer:
                value = func(value, item)
                yield cast(R, value)


async def list(source: AnyIterable[R]) -> List[R]:
    result = []
    async with aitercontext(source) as streamer:
        async for item in streamer:
            result.append(item)
    return result


def reduce(
    func: Callable[[Union[T, R], T], Union[Coroutine[Any, Any, R], R]],
    source: AnyIterable[T],
    initial: Union[R, Sentinel] = Unset,
) -> AsyncIterator[R]:
    acc = accumulate(source, cast(Any, func), initial=initial)
    return item(acc, -1)


async def chain(*sources: AnyIterable[T]) -> AsyncIterator[T]:
    for source in sources:
        async with aitercontext(source) as streamer:
            async for item in streamer:
                yield item


def chain_from_iterable(source: AnyIterable[AnyIterable[T]]) -> AsyncIterator[T]:
    return chain(concat(source))


@overload
def zip(source0: AnyIterable[T0], source1: AnyIterable[T1], /) -> AsyncIterator[Tuple[T0, T1]]:
    ...


@overload
def zip(
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    /,
) -> AsyncIterator[Tuple[T0, T1, T2]]:
    ...


@overload
def zip(
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    /,
) -> AsyncIterator[Tuple[T0, T1, T2, T3]]:
    ...


@overload
def zip(
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    /,
) -> AsyncIterator[Tuple[T0, T1, T2, T3, T4]]:
    ...


@overload
def zip(
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    source5: AnyIterable[T5],
    /,
    *sources: AnyIterable[Any],
) -> AsyncIterator[Tuple[T0, T1, T2, T3, T4, T5, Unpack[Tuple[Any, ...]]]]:
    ...


async def zip(*sources: AnyIterable[Any]) -> AsyncIterator[Tuple[Any, ...]]:
    async with AsyncExitStack() as stack:
        streamers = [await stack.enter_async_context(aitercontext(source)) for source in sources]
        while True:
            try:
                items = await asyncio.gather(*[anext(streamer) for streamer in streamers])
            except StopAsyncIteration:
                break
            else:
                yield tuple(items)


@overload
def smap(
    func: Callable[[T], R],
    source: AnyIterable[T],
    /,
    *,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def smap(
    func: Callable[[T0, T1], R],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    /,
    *,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def smap(
    func: Callable[[T0, T1, T2], R],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    /,
    *,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def smap(
    func: Callable[[T0, T1, T2, T3], R],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    /,
    *,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def smap(
    func: Callable[[T0, T1, T2, T3, T4], R],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    /,
    *,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def smap(
    func: Callable[[T0, T1, T2, T3, T4, T5, Unpack[Tuple[Any, ...]]], R],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    source5: AnyIterable[T5],
    /,
    *sources: AnyIterable[Any],
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


async def smap(
    func: Callable[..., Any],
    source: AnyIterable[Any],
    *sources: AnyIterable[Any],
    ignore: Tuple[Type[BaseException], ...] = (),
) -> AsyncIterator[Any]:
    if sources:
        src = zip(source, *sources)
        async with aitercontext(src) as streamer:
            async for item in streamer:
                try:
                    yield func(*item)
                except ignore:
                    pass
    else:
        src = anyiter(source)
        async with aitercontext(src) as streamer:
            async for item in streamer:
                try:
                    yield func(item)
                except ignore:
                    pass


@overload
def amap(
    func: Callable[[T], Coroutine[Any, Any, R]],
    source: AnyIterable[T],
    /,
    *,
    ordered: bool = ...,
    task_limit: Optional[int] = ...,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def amap(
    func: Callable[[T0, T1], Coroutine[Any, Any, R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    /,
    *,
    ordered: bool = ...,
    task_limit: Optional[int] = ...,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def amap(
    func: Callable[[T0, T1, T2], Coroutine[Any, Any, R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    /,
    *,
    ordered: bool = ...,
    task_limit: Optional[int] = ...,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def amap(
    func: Callable[[T0, T1, T2, T3], Coroutine[Any, Any, R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    /,
    *,
    ordered: bool = ...,
    task_limit: Optional[int] = ...,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def amap(
    func: Callable[[T0, T1, T2, T3, T4], Coroutine[Any, Any, R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    /,
    *,
    ordered: bool = ...,
    task_limit: Optional[int] = ...,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def amap(
    func: Callable[[T0, T1, T2, T3, T4, T5, Unpack[Tuple[Any, ...]]], Coroutine[Any, Any, R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    source5: AnyIterable[T5],
    /,
    *sources: AnyIterable[Any],
    ordered: bool = ...,
    task_limit: Optional[int] = ...,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


def amap(
    func: Callable[..., Coroutine[Any, Any, Any]],
    *sources: AnyIterable[Any],
    ordered: bool = True,
    task_limit: Optional[int] = None,
    ignore: Tuple[Type[BaseException], ...] = (),
) -> AsyncIterator[Any]:
    def fn(*args):
        return call(func, *args)

    if ordered:
        return concat(smap(fn, *sources, ignore=ignore), task_limit=task_limit)
    return flatten(smap(fn, *sources, ignore=ignore), task_limit=task_limit)


@overload
def map(
    func: Callable[[T], Union[Coroutine[Any, Any, R], R]],
    source: AnyIterable[T],
    /,
    *,
    ordered: bool = ...,
    task_limit: Optional[int] = ...,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def map(
    func: Callable[[T0, T1], Union[Coroutine[Any, Any, R], R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    /,
    *,
    ordered: bool = ...,
    task_limit: Optional[int] = ...,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def map(
    func: Callable[[T0, T1, T2], Union[Coroutine[Any, Any, R], R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    /,
    *,
    ordered: bool = ...,
    task_limit: Optional[int] = ...,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def map(
    func: Callable[[T0, T1, T2, T3], Union[Coroutine[Any, Any, R], R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    /,
    *,
    ordered: bool = ...,
    task_limit: Optional[int] = ...,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def map(
    func: Callable[[T0, T1, T2, T3, T4], Union[Coroutine[Any, Any, R], R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    /,
    *,
    ordered: bool = ...,
    task_limit: Optional[int] = ...,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


@overload
def map(
    func: Callable[[T0, T1, T2, T3, T4, T5, Unpack[Tuple[Any, ...]]], Union[Coroutine[Any, Any, R], R]],
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    source5: AnyIterable[T5],
    /,
    *sources: AnyIterable[Any],
    ordered: bool = ...,
    task_limit: Optional[int] = ...,
    ignore: Tuple[Type[BaseException], ...] = ...,
) -> AsyncIterator[R]:
    ...


def map(
    func: Callable[..., Any],
    *sources: AnyIterable[Any],
    ordered: bool = True,
    task_limit: Optional[int] = None,
    ignore: Tuple[Type[BaseException], ...] = (),
):
    if is_coro_function(func):
        return amap(func, *sources, ordered=ordered, task_limit=task_limit, ignore=ignore)
    return smap(func, *sources, ignore=ignore)


def merge(*sources: AnyIterable[T]) -> AsyncIterator[T]:
    return flatten(iterate(sources))


async def action(
    func: Callable[[T], Union[Coroutine[Any, Any, Any], Any]],
    source: AnyIterable[T],
) -> AsyncIterator[T]:
    if is_coro_function(func):
        async for item in anyiter(source):
            await func(item)
            yield item
    else:
        async for item in anyiter(source):
            func(item)
            yield item


def print(source: AnyIterable[T], template: Optional[str] = None, *args: Any, **kwargs: Any) -> AsyncIterator[T]:
    def func(value) -> None:
        builtins.print(template.format(value) if template else value, *args, **kwargs)

    return action(func, source)


async def take(source: AnyIterable[T], n: int) -> AsyncIterator[T]:
    async with aitercontext(enumerate(source)) as streamer:
        if n <= 0:
            return
        async for i, item in streamer:
            yield item
            if i >= n - 1:
                return


async def takelast(source: AnyIterable[T], n: int) -> AsyncIterator[T]:
    queue = collections.deque(maxlen=max(n, 0))
    async with aitercontext(source) as streamer:
        async for item in streamer:
            queue.append(item)
        for item in queue:
            yield item


async def skip(source: AnyIterable[T], n: int) -> AsyncIterator[T]:
    async with aitercontext(enumerate(source)) as streamer:
        async for i, item in streamer:
            if i >= n:
                yield item


async def skiplast(source: AnyIterable[T], n: int) -> AsyncIterator[T]:
    queue = collections.deque(maxlen=max(n, 0))
    async with aitercontext(source) as streamer:
        async for item in streamer:
            if n <= 0:
                yield item
                continue
            if len(queue) == n:
                yield queue[0]
            queue.append(item)


async def filterindex(func: Predicate[int], source: AnyIterable[T]) -> AsyncIterator[T]:
    async with aitercontext(enumerate(source)) as streamer:
        async for i, item in streamer:
            if func(i):
                yield item


def slice(source: AnyIterable[T], *args: Optional[SupportsIndex]) -> AsyncIterator[T]:
    s = builtins.slice(*args)
    start, stop, step = s.start or 0, s.stop, s.step or 1

    if start < 0:
        source = takelast(source, abs(start))
    elif start > 0:
        source = skip(source, start)

    if stop is not None:
        if stop >= 0 and start < 0:
            raise ValueError("Positive stop with negative start is not supported")
        elif stop >= 0:
            source = take(source, stop - start)
        else:
            source = skiplast(source, abs(stop))

    if step is not None:
        if step > 1:
            source = filterindex(lambda i: i % step == 0, source)
        elif step < 0:
            raise ValueError("Negative step not supported")
    # Return
    return anyiter(source)


async def item(source: AnyIterable[T], index: int) -> AsyncIterator[T]:
    source = skip(source, index) if index >= 0 else takelast(source, abs(index))
    async with aitercontext(source) as streamer:
        try:
            result = await anext(streamer)
        except StopAsyncIteration as e:
            raise IndexError("Index out of range") from e
        if index < 0:
            count = 1
            async for _ in streamer:
                count += 1
            if count != abs(index):
                raise IndexError("Index out of range")
        yield result


def getitem(source: AnyIterable[T], index: Union[int, builtins.slice]) -> AsyncIterator[T]:
    if isinstance(index, builtins.slice):
        return slice(source, index.start, index.stop, index.step)
    if isinstance(index, int):
        return item(source, index)
    raise TypeError("Not a valid index (int or slice)")


async def filter(pred: Predicate[T], source: AnyIterable[T]) -> AsyncIterator[T]:
    iscorofunc = asyncio.iscoroutinefunction(pred)
    async with aitercontext(source) as streamer:
        async for item in streamer:
            result = pred(item)
            if iscorofunc:
                result = await result
            if result:
                yield item


async def filterfalse(pred: Predicate[T], source: AnyIterable[T]) -> AsyncIterator[T]:
    async def negate(obj) -> bool:
        return not await maybe_await(pred(obj))

    async for x in filter(negate, source):
        yield x


async def until(func: Predicate[T], source: AnyIterable[T]) -> AsyncIterator[T]:
    iscorofunc = asyncio.iscoroutinefunction(func)
    async with aitercontext(source) as streamer:
        async for item in streamer:
            result = func(item)
            if iscorofunc:
                result = await result
            yield item
            if result:
                return


async def takewhile(func: Predicate[T], source: AnyIterable[T]) -> AsyncIterator[T]:
    iscorofunc = asyncio.iscoroutinefunction(func)
    async with aitercontext(source) as streamer:
        async for item in streamer:
            result = func(item)
            if iscorofunc:
                result = await result
            if not result:
                return
            yield item


async def dropwhile(func: Predicate[T], source: AnyIterable[T]) -> AsyncIterator[T]:
    iscorofunc = asyncio.iscoroutinefunction(func)
    async with aitercontext(source) as streamer:
        async for item in streamer:
            result = func(item)
            if iscorofunc:
                result = await result
            if not result:
                yield item
                break
        async for item in streamer:
            yield item


async def groupby(source: AnyIterable[Any], key_fn: Optional[KeyFunction[Any, Any]] = None):
    key = key_fn or identity
    grouping = []

    it = anyiter(source)
    try:
        item = await anext(it)
    except StopAsyncIteration:
        return
    grouping = [item]

    j = await maybe_await(key(item))
    async for item in it:
        k = await maybe_await(key(item))
        if k != j:
            yield j, grouping
            grouping = [item]
        else:
            grouping.append(item)
        j = k

    yield j, grouping


async def spaceout(source: AnyIterable[T], interval: float) -> AsyncIterator[T]:
    timeout = 0
    loop = asyncio.get_event_loop()
    async with aitercontext(source) as streamer:
        async for item in streamer:
            delta = timeout - loop.time()
            delay = max(delta, 0)
            await asyncio.sleep(delay)
            yield item
            timeout = loop.time() + interval


async def timeout(source: AnyIterable[T], timeout_seconds: float) -> AsyncIterator[T]:
    async with aitercontext(source) as streamer:
        while True:
            try:
                item = await asyncio.wait_for(anext(streamer), timeout_seconds)
            except StopAsyncIteration:
                break
            else:
                yield item


async def delay(source: AnyIterable[T], delay_seconds: float) -> AsyncIterator[T]:
    await asyncio.sleep(delay_seconds)
    async with aitercontext(source) as streamer:
        async for item in streamer:
            yield item


async def enumerate(source: AnyIterable[T], start: int = 0, step: int = 1) -> AsyncIterator[Tuple[int, T]]:
    count = itertools.count(start, step)
    async with aitercontext(source) as streamer:
        async for item in streamer:
            yield next(count), item


def starmap(
    func: Callable[..., Union[R, Coroutine[Any, Any, R]]],
    source: AnyIterable[Iterable[Any]],
    ordered: bool = True,
    task_limit: Optional[int] = None,
) -> AsyncIterator[R]:
    if is_coro_function(func):

        async def astarfunc(args: Iterable[Any]):
            return await func(*args)

        return map(astarfunc, source, ordered=ordered, task_limit=task_limit)
    else:

        def starfunc(args: Iterable[Any]):
            return func(*args)

        return map(starfunc, source, ordered=ordered, task_limit=task_limit)


async def chunks(source: AnyIterable[T], n: int) -> AsyncIterator[List[T]]:
    async with aitercontext(source) as streamer:
        async for first in streamer:
            xs = take(preserve(streamer), n - 1)
            yield [first] + [x async for x in xs]


def ziplatest(*sources: AnyIterable[Any], partial: bool = True, default: Optional[Any] = None):
    n = len(sources)

    def getter(dct: Mapping[Any, Any]):
        return lambda key: dct.get(key, default)

    new_sources = [smap(lambda x, i=i: {i: x}, source) for i, source in builtins.enumerate(sources)]
    merged = merge(*new_sources)
    accumulated = accumulate(merged, lambda x, e: {**x, **e})
    filtered = accumulated if partial else filter(lambda x: len(x) == n, accumulated)
    return smap(lambda x: tuple(builtins.map(getter(x), builtins.range(n))), filtered)


@overload
def ziplongest(
    source0: AnyIterable[T0],
    /,
    *,
    fillvalue: Any = ...,
) -> AsyncIterator[Tuple[T0]]:
    ...


@overload
def ziplongest(
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    /,
    *,
    fillvalue: A = None,
) -> AsyncIterator[Tuple[Union[T0, A], Union[T1, A]]]:
    ...


@overload
def ziplongest(
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    /,
    *,
    fillvalue: A = None,
) -> AsyncIterator[Tuple[Union[T0, A], Union[T1, A], Union[T2, A]]]:
    ...


@overload
def ziplongest(
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    /,
    *,
    fillvalue: A = None,
) -> AsyncIterator[Tuple[Union[T0, A], Union[T1, A], Union[T2, A], Union[T3, A]]]:
    ...


@overload
def ziplongest(
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    /,
    *,
    fillvalue: A = None,
) -> AsyncIterator[Tuple[Union[T0, A], Union[T1, A], Union[T2, A], Union[T3, A], Union[T4, A]]]:
    ...


@overload
def ziplongest(
    source0: AnyIterable[T0],
    source1: AnyIterable[T1],
    source2: AnyIterable[T2],
    source3: AnyIterable[T3],
    source4: AnyIterable[T4],
    source5: AnyIterable[T5],
    /,
    *,
    fillvalue: A = None,
) -> AsyncIterator[Tuple[Union[T0, A], Union[T1, A], Union[T2, A], Union[T3, A], Union[T4, A], Union[T5, A]]]:
    ...


@overload
def ziplongest(
    source0: AnyIterable[Any],
    source1: AnyIterable[Any],
    source2: AnyIterable[Any],
    source3: AnyIterable[Any],
    source4: AnyIterable[Any],
    /,
    *sources: AnyIterable[Any],
    fillvalue: A = None,
) -> AsyncIterator[Tuple[Union[Any, A], ...]]:
    ...


async def ziplongest(*sources: AnyIterable[Any], fillvalue: A = None) -> AsyncIterator[Tuple[Union[Any, A], ...]]:
    its: List[AsyncIterator[Any]] = [anyiter(itr) for itr in sources]
    itr_count = len(its)
    finished = 0

    while True:
        values = [x for x in await asyncio.gather(*[anext(it) for it in its], return_exceptions=True)]
        for idx, value in builtins.enumerate(values):
            if isinstance(value, AnyStop):
                finished += 1
                values[idx] = fillvalue
                its[idx] = repeat(fillvalue)
            elif isinstance(value, BaseException):
                raise value
        if finished >= itr_count:
            break
        yield cast(Any, tuple(values))


@overload
def tee(source: AnyIterable[T], n: Literal[2] = ...) -> Tuple[AsyncIterator[T], AsyncIterator[T]]:
    ...


@overload
def tee(source: AnyIterable[T], n: Literal[3] = ...) -> Tuple[AsyncIterator[T], AsyncIterator[T], AsyncIterator[T]]:
    ...


@overload
def tee(
    source: AnyIterable[T], n: Literal[4] = ...
) -> Tuple[AsyncIterator[T], AsyncIterator[T], AsyncIterator[T], AsyncIterator[T]]:
    ...


@overload
def tee(
    source: AnyIterable[T], n: Literal[5] = ...
) -> Tuple[AsyncIterator[T], AsyncIterator[T], AsyncIterator[T], AsyncIterator[T], AsyncIterator[T]]:
    ...


@overload
def tee(
    source: AnyIterable[T], n: Literal[6] = ...
) -> Tuple[AsyncIterator[T], AsyncIterator[T], AsyncIterator[T], AsyncIterator[T], AsyncIterator[T], AsyncIterator[T]]:
    ...


@overload
def tee(source: AnyIterable[T], n: int = ...) -> Tuple[AsyncIterator[T], ...]:
    ...


def tee(source: AnyIterable[T], n: int = 2, *, concurrent: bool = False) -> Tuple[AsyncIterator[T], ...]:
    it = aitercontext(source)
    peers: List[Deque[Union[T, Exception]]] = [collections.deque() for _ in builtins.range(n)]
    lock = NoLock() if concurrent else asyncio.Lock()
    streamer: Optional[AsyncIterator[T]] = None

    async def tee_peer(buffer: Deque[Union[Exception, T]]) -> AsyncGenerator[T, None]:
        nonlocal streamer

        try:
            while True:
                if not buffer:
                    async with lock:
                        # Another peer produced an item while we were waiting for the lock.
                        # Proceed with the next loop iteration to yield the item.
                        if buffer:
                            continue

                        if streamer is None:
                            streamer = await it.__aenter__()

                        try:
                            item = await anext(streamer)
                        except StopAsyncIteration:
                            break
                        except Exception as e:
                            item = e

                        # Append to all buffers, including our own. We'll fetch our
                        # item from the buffer again, instead of yielding it directly.
                        # This ensures the proper item ordering if any of our peers
                        # are fetching items concurrently. They may have buffered their
                        # item already.
                        for peer_buffer in peers:
                            peer_buffer.append(item)
                result = buffer.popleft()
                if isinstance(result, Exception):
                    raise result
                yield result
        finally:
            peers.remove(buffer)
            # if we are the last peer, try and close the iterator
            if not peers:
                if streamer is not None:
                    await it.__aexit__(None, None, None)
                    streamer = None

    return tuple(tee_peer(buffer) for buffer in peers)


def partition(source: AnyIterable[T], pred: Predicate[T] = bool) -> Tuple[AsyncIterator[T], AsyncIterator[T]]:
    def peer(it: AsyncIterator[Tuple[Union[Awaitable[bool], bool], T]], flag: bool) -> AsyncIterator[T]:
        return (x async for cond, x in it if cond == flag)

    async def evaluate(it: AsyncIterator[T]) -> AsyncIterator[Tuple[bool, T]]:
        async with aitercontext(it) as it:
            async for x in it:
                cond = pred(x)
                if inspect.isawaitable(cond):
                    cond = await cond
                yield cast(bool, cond), x

    t1, t2 = tee(evaluate(anyiter(source)))
    return (peer(t1, True), peer(t2, False))
