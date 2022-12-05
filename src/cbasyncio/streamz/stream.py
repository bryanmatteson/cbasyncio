from __future__ import annotations

import asyncio
import builtins
import functools
import inspect
import io
import operator as op
from contextlib import AsyncExitStack
from types import TracebackType
from typing import (
    IO,
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    Generator,
    Generic,
    Iterable,
    List,
    Optional,
    Set,
    SupportsIndex,
    Tuple,
    Type,
    Union,
    cast,
    overload,
)

from typing_extensions import Concatenate, Self, Unpack, assert_never

from cbasyncio.types import Unset

from .._types import P1, T0, T1, T2, T3, T4, T5, A, AnyFunction, AnyIterable, B, KeyFunction, P, Predicate, R, T
from ..context import AsyncIteratorContext, aitercontext
from ..fileio import open_file, wrap_file
from ..utils import anext, anyiter, assert_async_iterable
from . import toolz
from .channel import RecvChannel


class _sentinel:
    pass


async def wait_stream(it: AsyncIterable[T]) -> T:
    async with streamcontext(it) as streamer:
        last_item = _sentinel()
        async for item in streamer:
            last_item = item
        if isinstance(last_item, _sentinel):
            raise StreamEmpty()
        return last_item


class StreamerManager:
    def __init__(self):
        self.tasks: Dict[Streamer, asyncio.Task] = {}
        self.streamers: List[Streamer] = []
        self.group = StreamerGroup()
        self.stack = AsyncExitStack()

    async def __aenter__(self) -> Self:
        await self.stack.__aenter__()
        await self.stack.enter_async_context(self.group)
        return self

    async def __aexit__(
        self,
        typ: Optional[Type[BaseException]] = None,
        value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> Optional[bool]:
        for streamer in self.streamers:
            task = self.tasks.pop(streamer, None)
            if task is not None:
                self.stack.push_async_callback(self.group.cancel_task, task)
            self.stack.push_async_exit(streamer)
        self.tasks.clear()
        self.streamers.clear()
        return await self.stack.__aexit__(typ, value, traceback)

    async def enter_and_create_task(self, it: AsyncIterable[T]) -> Streamer[T]:
        streamer = streamcontext(it)
        await streamer.__aenter__()
        self.streamers.append(streamer)
        self.create_task(streamer)
        return streamer

    def create_task(self, streamer: Streamer):
        assert streamer in self.streamers
        assert streamer not in self.tasks
        self.tasks[streamer] = self.group.create_task(anext(streamer))

    async def wait_single_event(self, filters: Iterable[Streamer]) -> Tuple[Streamer, asyncio.Task]:
        tasks = [self.tasks[streamer] for streamer in filters]
        done = await self.group.wait_any(tasks)
        for streamer in filters:
            if self.tasks.get(streamer) in done:
                return streamer, self.tasks.pop(streamer)
        assert_never()

    async def clean_streamer(self, streamer: Streamer) -> None:
        task = self.tasks.pop(streamer, None)
        if task is not None:
            await self.group.cancel_task(task)
        await streamer.aclose()
        self.streamers.remove(streamer)

    async def clean_streamers(self, streamers: Iterable[Streamer]):
        tasks = [self.group.create_task(self.clean_streamer(streamer)) for streamer in streamers]
        done = await self.group.wait_all(tasks)
        # Raise exception if any
        for task in done:
            task.result()


class StreamerGroup:
    def __init__(self):
        self._pending: Set[asyncio.Task] = set()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args) -> Optional[bool]:
        while self._pending:
            task = self._pending.pop()
            await self.cancel_task(task)

    def create_task(self, coro: Coroutine[Any, Any, T]) -> "asyncio.Task[T]":
        task = asyncio.ensure_future(coro)
        self._pending.add(task)
        return task

    async def wait_any(self, tasks: Iterable[Awaitable[T]]) -> "Set[asyncio.Task[T]]":
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        self._pending -= done
        return done

    async def wait_all(self, tasks: Iterable[Awaitable[T]]) -> "Set[asyncio.Task[T]]":
        if not tasks:
            return set()
        done, _ = await asyncio.wait(tasks)
        self._pending -= done
        return done

    async def cancel_task(self, task: asyncio.Task) -> None:
        try:
            if task.cancelled():
                pass
            elif task.done():
                task.exception()
            else:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
        finally:
            self._pending.discard(task)


class StreamEmpty(Exception):
    """Exception raised when awaiting an empty stream."""


class Stream(AsyncIterable[R]):
    def __init__(self, factory: Callable[[], AsyncIterable[R]]):
        it = factory()
        assert_async_iterable(it)
        self._generator = self._make_generator(it, factory)

    def _make_generator(self, first: AsyncIterable[R], factory: Callable[[], AsyncIterable[R]]):
        yield first
        del first
        while True:
            yield factory()

    def __aiter__(self) -> Streamer[R]:
        return streamcontext(next(self._generator))

    def __await__(self) -> Generator[Any, None, R]:
        return wait_stream(self).__await__()

    __iter__ = None

    def stream(self) -> Streamer[R]:
        return self.__aiter__()

    __aexit__ = None

    async def __aenter__(self):
        raise TypeError(
            "A stream object cannot be used as a context manager. "
            "Use the `stream` method instead: "
            "`async with xs.stream() as streamer`"
        )

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __or__(self, func: Callable[[Stream[R]], Stream[T]]) -> Stream[T]:
        return func(self)

    def __add__(self, value: Stream[T]) -> Stream[Union[R, T]]:
        from .stream import chain

        return chain(self, value)

    def __getitem__(self, value):
        from .stream import getitem

        return getitem(self, value)

    @classmethod
    def chained(cls, *sources: AnyIterable[T]) -> Stream[T]:
        return chain(*sources)

    @classmethod
    def from_channel(cls, channel: RecvChannel[T]) -> Stream[T]:
        return from_channel(channel)

    @classmethod
    def iterate(cls, it: AnyIterable[T]) -> Stream[T]:
        return iterate(it)

    @classmethod
    def preserve(cls, it: AsyncIterable[T]) -> Stream[T]:
        return preserve(it)

    @classmethod
    def just(cls, value: Union[T, Awaitable[T]]) -> Stream[T]:
        return just(value)

    @classmethod
    def call(cls, func: AnyFunction[T], *args: Any, **kwargs: Any) -> Stream[T]:
        return call(func, *args, **kwargs)

    @classmethod
    def throw(cls, exc: Union[BaseException, Type[BaseException]]) -> Stream[None]:
        return throw(exc)

    @classmethod
    def empty(cls) -> Stream[None]:
        return empty()

    @classmethod
    def never(cls) -> Stream[None]:
        return never()

    @classmethod
    def repeat(cls, value: T, times: Optional[int] = None, *, interval: float = 0) -> Stream[T]:
        return repeat(value, times, interval=interval)

    @classmethod
    def range(cls, *args: SupportsIndex, interval: float = 0) -> Stream[int]:
        return range(*args, interval=interval)

    @classmethod
    def count(cls, start: int = 0, step: int = 1, *, interval: float = 0) -> Stream[int]:
        return count(start, step, interval=interval)

    def concat(self: Stream[AnyIterable[T]], task_limit: Optional[int] = None) -> Stream[T]:
        return concat(self, task_limit=task_limit)

    def flatten(self: Stream[AnyIterable[T]], task_limit: Optional[int] = None) -> Stream[T]:
        return flatten(self, task_limit=task_limit)

    def switch(self: Stream[AnyIterable[T]]) -> Stream[T]:
        return switch(self)

    def concatmap(
        self,
        func: Callable[[Union[R, B]], AnyIterable[T]],
        *sources: AnyIterable[B],
        task_limit: Optional[int] = None,
    ) -> Stream[T]:
        return concatmap(self, func, *sources, task_limit=task_limit)

    def flatmap(
        self,
        func: Callable[[Union[R, B]], AnyIterable[T]],
        *sources: AnyIterable[B],
        task_limit: Optional[int] = None,
    ) -> Stream[T]:
        return flatmap(self, func, *sources, task_limit=task_limit)

    def switchmap(self, func: Callable[[Union[R, B]], AnyIterable[T]], *sources: AnyIterable[B]) -> Stream[T]:
        return switchmap(self, func, *sources)

    def accumulate(self, func: Callable[[A, R], T] = op.add, initial: A = None) -> Stream[T]:
        return accumulate(self, func, initial=initial)

    def list(self) -> Stream[List[R]]:
        return list(
            self,
        )

    def reduce(self, func: Callable[[A, R], T] = op.add, initial: A = None) -> Stream[T]:
        return reduce(self, func, initial=initial)

    def chain(self, *sources: AnyIterable[T]) -> Stream[Union[R, T]]:
        return chain(self, *sources)

    def merge(self, *sources: AnyIterable[T]) -> Stream[Union[R, T]]:
        return merge(self, *sources)

    def action(self, func: Callable[[R], Union[Awaitable[Any], Any]]) -> Stream[R]:
        return action(self, func)

    def print(self, template: Optional[str] = None, *args: Any, **kwargs: Any) -> Stream[R]:
        return print(self, template=template, *args, **kwargs)

    def take(self, n: int) -> Stream[R]:
        return take(self, n)

    def takelast(self, n: int) -> Stream[R]:
        return takelast(self, n)

    def skip(self, n: int) -> Stream[R]:
        return skip(self, n)

    def skiplast(self, n: int) -> Stream[R]:
        return skiplast(self, n)

    def filterindex(self, pred: Predicate[int]) -> Stream[R]:
        return filterindex(self, pred)

    def slice(self, *args: Optional[int]) -> Stream[R]:
        return slice(self, *args)

    def item(self, n: int) -> Stream[R]:
        return item(self, n)

    def getitem(self, n: int) -> Stream[R]:
        return getitem(self, n)

    def filter(self, pred: Predicate[R]) -> Stream[R]:
        return filter(self, pred)

    def filterfalse(self, pred: Predicate[R]) -> Stream[R]:
        return filterfalse(self, pred)

    def until(self, pred: Predicate[R]) -> Stream[R]:
        return until(self, pred)

    def takewhile(self, pred: Predicate[R]) -> Stream[R]:
        return takewhile(self, pred)

    def dropwhile(self, pred: Predicate[R]) -> Stream[R]:
        return dropwhile(self, pred)

    def spaceout(self, interval: float) -> Stream[R]:
        return spaceout(self, interval)

    def timeout(self, seconds: float) -> Stream[R]:
        return timeout(self, seconds)

    def delay(self, seconds: float) -> Stream[R]:
        return delay(self, seconds)

    def enumerate(self, start: int = 0, step: int = 1) -> Stream[Tuple[int, R]]:
        return enumerate(self, start, step)

    def starmap(
        self, func: Callable[..., Union[Awaitable[T], T]], ordered: bool = True, task_limit: Optional[int] = None
    ) -> Stream[T]:
        return starmap(self, func, ordered, task_limit=task_limit)

    def chunks(self, n: int) -> Stream[List[R]]:
        return chunks(self, n)

    def groupby(self, key: KeyFunction[R, T]) -> Stream[Tuple[T, List[R]]]:
        return groupby(self, key)

    def split(self, n: int) -> Tuple[Stream[R], ...]:
        return tuple(Streamer(it) for it in toolz.tee(self, n))

    def partition(self, pred: Predicate[R]) -> Tuple[Stream[R], Stream[R]]:
        true_it, false_it = toolz.partition(self, pred)
        return (Streamer(true_it), Streamer(false_it))

    @overload
    def map(
        self,
        func: Callable[[R], Union[Coroutine[Any, Any, T], T]],
        /,
        *,
        ordered: bool = ...,
        task_limit: Optional[int] = ...,
    ) -> Stream[T]:
        ...

    @overload
    def map(
        self,
        func: Callable[[R, T0], Union[Coroutine[Any, Any, T], T]],
        source0: AnyIterable[T0],
        /,
        *,
        ordered: bool = ...,
        task_limit: Optional[int] = ...,
    ) -> Stream[T]:
        ...

    @overload
    def map(
        self,
        func: Callable[[R, T0, T1], Union[Coroutine[Any, Any, T], T]],
        source0: AnyIterable[T0],
        source1: AnyIterable[T1],
        /,
        *,
        ordered: bool = ...,
        task_limit: Optional[int] = ...,
    ) -> Stream[T]:
        ...

    @overload
    def map(
        self,
        func: Callable[[R, T0, T1, T2], Union[Coroutine[Any, Any, T], T]],
        source0: AnyIterable[T0],
        source1: AnyIterable[T1],
        source2: AnyIterable[T2],
        /,
        *,
        ordered: bool = ...,
        task_limit: Optional[int] = ...,
    ) -> Stream[T]:
        ...

    @overload
    def map(
        self,
        func: Callable[[R, T0, T1, T2, T3], Union[Coroutine[Any, Any, T], T]],
        source0: AnyIterable[T0],
        source1: AnyIterable[T1],
        source2: AnyIterable[T2],
        source3: AnyIterable[T3],
        /,
        *,
        ordered: bool = ...,
        task_limit: Optional[int] = ...,
    ) -> Stream[T]:
        ...

    @overload
    def map(
        self,
        func: Callable[[R, T0, T1, T2, T3, T4], Union[Coroutine[Any, Any, T], T]],
        source0: AnyIterable[T0],
        source1: AnyIterable[T1],
        source2: AnyIterable[T2],
        source3: AnyIterable[T3],
        source4: AnyIterable[T4],
        /,
        *,
        ordered: bool = ...,
        task_limit: Optional[int] = ...,
    ) -> Stream[T]:
        ...

    @overload
    def map(
        self,
        func: Callable[[R, T0, T1, T2, T3, T4, T5, Unpack[Tuple[Any, ...]]], Union[Coroutine[Any, Any, T], T]],
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
    ) -> Stream[T]:
        ...

    def map(
        self,
        fn: Callable[..., Union[Coroutine[Any, Any, T], T]],
        /,
        *sources: AnyIterable[Any],
        ordered: bool = True,
        task_limit: Optional[int] = None,
    ) -> Stream[T]:
        return map(self, fn, *sources, ordered=ordered, task_limit=task_limit)

    @overload
    def zip(self, source0: AnyIterable[T0], /) -> Stream[Tuple[R, T0]]:
        ...

    @overload
    def zip(self, source0: AnyIterable[T0], source1: AnyIterable[T1], /) -> Stream[Tuple[R, T0, T1]]:
        ...

    @overload
    def zip(
        self,
        source0: AnyIterable[T0],
        source1: AnyIterable[T1],
        source2: AnyIterable[T2],
        /,
    ) -> Stream[Tuple[R, T0, T1, T2]]:
        ...

    @overload
    def zip(
        self,
        source0: AnyIterable[T0],
        source1: AnyIterable[T1],
        source2: AnyIterable[T2],
        source3: AnyIterable[T3],
        /,
    ) -> Stream[Tuple[R, T0, T1, T2, T3]]:
        ...

    @overload
    def zip(
        self,
        source0: AnyIterable[T0],
        source1: AnyIterable[T1],
        source2: AnyIterable[T2],
        source3: AnyIterable[T3],
        source4: AnyIterable[T4],
        /,
    ) -> Stream[Tuple[R, T0, T1, T2, T3, T4]]:
        ...

    @overload
    def zip(
        self,
        source0: AnyIterable[T0],
        source1: AnyIterable[T1],
        source2: AnyIterable[T2],
        source3: AnyIterable[T3],
        source4: AnyIterable[T4],
        source5: AnyIterable[T5],
        /,
        *sources: AnyIterable[Any],
    ) -> Stream[Tuple[R, T0, T1, T2, T3, T4, T5, Unpack[Tuple[Any, ...]]]]:
        ...

    def zip(self, /, *sources: AnyIterable[Any]) -> Stream[Tuple[Any, ...]]:
        return zip(self, *sources)


class Streamer(AsyncIteratorContext[T], Stream[T]):
    pass


def streamcontext(it: AnyIterable[T]) -> Streamer[T]:
    it = anyiter(it)
    if isinstance(it, AsyncIteratorContext):
        it = it._it

    return it if isinstance(it, Streamer) else Streamer(it)


class Operator(Stream[R], Generic[P, R]):
    def __init__(self: Operator[P, R], *args: P.args, **kwargs: P.kwargs) -> None:
        raise NotImplementedError

    @classmethod
    def raw(cls: Type[Operator[P, A]], *args: P.args, **kwargs: P.kwargs) -> AsyncIterator[A]:
        raise NotImplementedError

    @classmethod
    def pipe(
        cls: Type[Operator[Concatenate[T, P1], A]], *args: P1.args, **kwargs: P1.kwargs
    ) -> Callable[[T], Stream[A]]:
        raise NotImplementedError


@overload
def operator(*, pipable: bool = ...) -> Callable[[Callable[P, AsyncIterator[T]]], Type[Operator[P, T]]]:
    ...


@overload
def operator(func: Callable[P, AsyncIterator[T]], /) -> Type[Operator[P, T]]:
    ...


def operator(func: Optional[Callable[P, AsyncIterator[T]]] = None, *, pipable: bool = False) -> Any:
    def decorator(func: Callable[..., Any]) -> Type[Operator]:
        if isinstance(func, classmethod):
            raise ValueError(
                "An operator cannot be created from a class method, "
                "since the decorated function becomes an operator class"
            )

        bases = (Operator,)
        name = func.__name__
        module = func.__module__
        extra_doc = func.__doc__
        doc = extra_doc or f"Regular {name} stream operator."

        # Extract signature
        signature = inspect.signature(func)
        parameters = builtins.list(signature.parameters.values())
        if parameters and parameters[0].name in ("self", "cls"):
            raise ValueError(
                "An operator cannot be created from a method, " "since the decorated function becomes an operator class"
            )

        # Injected parameters
        self_parameter = inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD)
        cls_parameter = inspect.Parameter("cls", inspect.Parameter.POSITIONAL_OR_KEYWORD)

        # Raw static method
        raw = func
        raw.__qualname__ = name + ".raw"

        # Init method
        def init(self, *args, **kwargs):
            if pipable and args:
                assert_async_iterable(args[0])
            factory = functools.partial(self.raw, *args, **kwargs)
            return Stream.__init__(self, factory)

        # Customize init signature
        new_parameters = [self_parameter] + parameters
        init.__signature__ = signature.replace(parameters=new_parameters)

        # Customize init method
        init.__qualname__ = name + ".__init__"
        init.__name__ = "__init__"
        init.__module__ = module
        init.__doc__ = f"Initialize the {name} stream."

        attrs = {"__init__": init, "__module__": module, "__doc__": doc}

        if pipable:
            raw.__signature__ = signature
            raw.__qualname__ = name + ".raw"
            raw.__module__ = module
            raw.__doc__ = doc

            # Pipe class method
            def pipe(cls, *args, **kwargs):
                return lambda *sources: cls(*sources, *args, **kwargs)

            # Customize pipe signature
            if parameters and parameters[0].kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                new_parameters = [cls_parameter] + parameters[1:]
            else:
                new_parameters = [cls_parameter] + parameters
            pipe.__signature__ = signature.replace(parameters=new_parameters)
            pipe.__qualname__ = name + ".pipe"
            pipe.__module__ = module
            pipe.__doc__ = f'Pipable "{name}" stream operator.'
            if extra_doc:
                pipe.__doc__ += "\n\n    " + extra_doc
            attrs["pipe"] = classmethod(pipe)
        attrs["raw"] = staticmethod(raw)

        return cast(Any, type(name, bases, attrs))

    return decorator if func is None else decorator(func)


@operator
async def from_channel(source: RecvChannel[Any]):
    async with source.clone() as chan:
        async for item in chan:
            yield item


@operator
async def from_file(filename: str, chunk_size: int = io.DEFAULT_BUFFER_SIZE) -> AsyncIterator[bytes]:
    async with await open_file(filename, "rb") as fp:
        async for chunk in fp.aiter_bytes(chunk_size):
            yield chunk


@operator
async def file_fileobj(file: IO[bytes], chunk_size: int = io.DEFAULT_BUFFER_SIZE) -> AsyncIterator[bytes]:
    async with wrap_file(file) as fp:
        async for chunk in fp.aiter_bytes(chunk_size):
            yield chunk


@operator
def from_iterable(it: Iterable[Any]):
    return toolz.from_iterable(it)


@operator
def from_async_iterable(ait: AsyncIterable[Any]):
    assert_async_iterable(ait)
    return toolz.from_async_iterable(ait)


@operator
def iterate(it: AnyIterable[Any]):
    return toolz.iterate(it)


@operator
def preserve(ait: AsyncIterable[Any]):
    return toolz.preserve(ait)


@operator
def just(value: Any):
    return toolz.just(value)


@operator
def call(func: Callable[..., Any], *args: Any, **kwargs: Any):
    return toolz.call(func, *args, **kwargs)


@operator
def throw(exc: Union[Type[BaseException], BaseException]):
    return toolz.throw(exc)


@operator
def empty():
    return toolz.empty()


@operator
def never():
    return toolz.never()


@operator
def repeat(value: Any, times: Optional[int] = None, *, interval: float = 0.0):
    return toolz.repeat(value, times, interval=interval)


@operator
def range(*args: SupportsIndex, interval: float = 0.0):
    return toolz.range(*args, interval=interval)


@operator
def count(start: int = 0, step: int = 1, *, interval: float = 0.0):
    return toolz.count(start, step, interval=interval)


@operator(pipable=True)
def concat(source: AnyIterable[AnyIterable[Any]], task_limit: Optional[int] = None):
    return toolz.concat(source, task_limit=task_limit)


@operator(pipable=True)
def flatten(source: AnyIterable[AnyIterable[Any]], task_limit: Optional[int] = None):
    return toolz.flatten(source, task_limit=task_limit)


@operator(pipable=True)
def switch(source: AnyIterable[AnyIterable[Any]]):
    return toolz.switch(source)


@operator(pipable=True)
def concatmap(
    source: AnyIterable[Any],
    func: Callable[..., Any],
    *sources: AnyIterable[Any],
    task_limit: Optional[int] = None,
):
    return concat.raw(smap.raw(source, func, *sources), task_limit=task_limit)


@operator(pipable=True)
def flatmap(
    source: AnyIterable[Any],
    func: Callable[..., Any],
    *sources: AnyIterable[Any],
    task_limit: Optional[int] = None,
):
    return flatten.raw(smap.raw(source, func, *sources), task_limit=task_limit)


@operator(pipable=True)
def switchmap(source: AnyIterable[Any], func: Callable[..., Any], *sources: AnyIterable[Any]):
    return switch.raw(smap.raw(source, func, *sources))


@operator(pipable=True)
def accumulate(source: AnyIterable[Any], func: Callable[..., Any] = op.add, initial: Optional[Any] = Unset):
    return toolz.accumulate(source, func, initial=initial)


@operator(pipable=True)
async def list(source: AnyIterable[Any]) -> AsyncIterator[List[Any]]:
    result = []
    yield result
    async with aitercontext(source) as streamer:
        async for item in streamer:
            result.append(item)
            yield result


@operator(pipable=True)
def reduce(source: AnyIterable[Any], func: Callable[..., Any], initial: Optional[Any] = Unset):
    return toolz.reduce(func, source, initial=initial)


@operator(pipable=True)
def chain(source: AnyIterable[Any], *sources: AnyIterable[Any]):
    return toolz.chain(source, *sources)


@operator(pipable=True)
def chain_from_iterable(source: AnyIterable[Any]):
    return toolz.chain_from_iterable(source)


@operator(pipable=True)
def zip(source: AnyIterable[Any], *sources: AnyIterable[Any]):
    return toolz.zip(source, *sources)


@operator(pipable=True)
def ziplatest(source: AnyIterable[Any], *sources: AnyIterable[Any], partial=True, default=None):
    return toolz.ziplatest(source, *sources, partial=partial, default=default)


@operator(pipable=True)
def ziplongest(source: AnyIterable[Any], *sources: AnyIterable[Any], fillvalue: Optional[Any] = None):
    return toolz.ziplongest(source, *sources, fillvalue=fillvalue)


@operator(pipable=True)
def smap(source: AnyIterable[Any], func: Callable[..., Any], *sources: AnyIterable[Any]):
    return toolz.smap(func, source, *sources)


@operator(pipable=True)
def amap(
    source: AnyIterable[Any],
    fn: Callable[..., Any],
    *sources: AnyIterable[Any],
    ordered: bool = True,
    task_limit: Optional[int] = None,
):
    return toolz.amap(fn, source, *sources, ordered=ordered, task_limit=task_limit)


@operator(pipable=True)
def map(
    source: AnyIterable[Any],
    func: Callable[..., Any],
    *sources: AnyIterable[Any],
    ordered: bool = True,
    task_limit: Optional[int] = None,
):
    return toolz.map(func, source, *sources, ordered=ordered, task_limit=task_limit)


@operator(pipable=True)
def merge(*sources: AnyIterable[Any]):
    return flatten.raw(iterate.raw(sources))


@operator(pipable=True)
def action(source: AnyIterable[Any], func: Callable[..., Any]):
    return toolz.action(func, source)


@operator(pipable=True)
def print(source: AnyIterable[Any], template: Optional[str] = None, *args: Any, **kwargs: Any):
    return toolz.print(source, template, *args, **kwargs)


@operator(pipable=True)
def take(source: AnyIterable[Any], n: int):
    return toolz.take(source, n)


@operator(pipable=True)
def takelast(source: AnyIterable[Any], n: int):
    return toolz.takelast(source, n)


@operator(pipable=True)
def skip(source: AnyIterable[Any], n: int):
    return toolz.skip(source, n)


@operator(pipable=True)
def skiplast(source: AnyIterable[Any], n: int):
    return toolz.skiplast(source, n)


@operator(pipable=True)
def filterindex(source: AnyIterable[Any], func: Predicate[Any]):
    return toolz.filterindex(func, source)


@operator(pipable=True)
def slice(source: AnyIterable[Any], *args: Any):
    return toolz.slice(source, *args)


@operator(pipable=True)
def item(source: Any, index: int):
    return toolz.item(source, index)


@operator(pipable=True)
def getitem(source: AnyIterable[Any], index: Union[int, builtins.slice]):
    return toolz.getitem(source, index)


@operator(pipable=True)
def filter(source: AnyIterable[Any], pred: Predicate[Any]):
    return toolz.filter(pred, source)


@operator(pipable=True)
def filterfalse(source: AnyIterable[Any], pred: Predicate[Any]):
    return toolz.filterfalse(pred, source)


@operator(pipable=True)
def until(source: AnyIterable[Any], func: Predicate[Any]):
    return toolz.until(func, source)


@operator(pipable=True)
def takewhile(source: AnyIterable[Any], func: Predicate[Any]):
    return toolz.takewhile(func, source)


@operator(pipable=True)
def dropwhile(source: AnyIterable[Any], func: Predicate[Any]):
    return toolz.dropwhile(func, source)


@operator(pipable=True)
def groupby(source: AnyIterable[Any], key_fn: Optional[KeyFunction[Any, Any]] = None):
    return toolz.groupby(source, key_fn)


@operator(pipable=True)
def spaceout(source: AnyIterable[Any], interval: float):
    return toolz.spaceout(source, interval)


@operator(pipable=True)
def timeout(source: AnyIterable[Any], timeout_seconds: float):
    return toolz.timeout(source, timeout_seconds)


@operator(pipable=True)
def delay(source: AnyIterable[Any], delay_seconds: float):
    return toolz.delay(source, delay_seconds)


@operator(pipable=True)
def enumerate(source: AnyIterable[Any], start: int = 0, step: int = 1):
    return toolz.enumerate(source, start, step)


@operator(pipable=True)
def starmap(source: AnyIterable[Any], func: Callable[..., Any], ordered: bool = True, task_limit: Optional[int] = None):
    return toolz.starmap(func, source, ordered=ordered, task_limit=task_limit)


@operator(pipable=True)
def chunks(source: AnyIterable[Any], n: int):
    return toolz.chunks(source, n)
