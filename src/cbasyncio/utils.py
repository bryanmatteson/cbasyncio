import asyncio
import inspect
import io
from collections import OrderedDict
from dataclasses import fields, is_dataclass
from functools import partial
from typing import (
    IO,
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
    Generic,
    Iterable,
    Iterator,
    Literal,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
    overload,
)
from unittest.mock import Mock

from typing_extensions import TypeGuard

from cbasyncio.gather import gather_tasks

try:
    from pydantic import BaseModel

    _has_pydantic = True
except ImportError:
    _has_pydantic = False

from ._types import P, T


async def aidentity(x: T) -> T:
    await asyncio.sleep(0)
    return x


def identity(x: T) -> T:
    return x


def is_async_iterable(obj: Union[Any, AsyncIterable[T]]) -> TypeGuard[AsyncIterable[T]]:
    return isinstance(obj, AsyncIterable)


def assert_async_iterable(obj: Any) -> None:
    if not is_async_iterable(obj):
        raise TypeError(f"{obj} is not an async iterable")


def is_async_iterator(obj: Union[Any, AsyncIterator[T]]) -> TypeGuard[AsyncIterator[T]]:
    return isinstance(obj, AsyncIterator)


async def maybe_await(o: Union[T, Awaitable[T]]) -> T:
    if inspect.isawaitable(o):
        return await o
    return cast(T, o)


_sentinel = object()


def is_coro(obj: Union[Any, Coroutine[Any, Any, T]]) -> TypeGuard[Coroutine[Any, Any, T]]:
    return inspect.iscoroutine(obj)


def is_coro_function(
    obj: Union[Any, Callable[P, Coroutine[Any, Any, T]]]
) -> TypeGuard[Callable[P, Coroutine[Any, Any, T]]]:
    return inspect.iscoroutinefunction(obj)


async def anext(it: AsyncIterator[T], /, default: Any = _sentinel) -> T:
    try:
        return await it.__anext__()
    except StopAsyncIteration:
        if default is _sentinel:
            raise
        return default


def aiter(obj: AsyncIterable[T]) -> AsyncIterator[T]:
    return obj.__aiter__()


def anyiter(obj: Union[Iterable[T], AsyncIterable[T]]) -> AsyncIterator[T]:
    if isinstance(obj, AsyncIterable):
        return aiter(obj)

    async def _iterator():
        for item in obj:
            await asyncio.sleep(0)
            yield item

    return _iterator()


@overload
async def visit_collection(expr: Any, visit: Callable[[Any], Coroutine], collect: Literal[False] = False) -> None:
    ...


@overload
async def visit_collection(expr: T, visit: Callable[[Any], Coroutine], collect: Literal[True] = True) -> T:
    ...


@overload
async def visit_collection(expr: T, visit: Callable[[Any], Coroutine], collect: bool = False) -> Optional[T]:
    ...


async def visit_collection(
    expr: T, visit: Callable[[Any], Coroutine], collect: bool = False, no_visit_types: Tuple[Type[Any], ...] = ()
) -> Optional[T]:
    def visit_nested(expr):
        return partial(visit_collection, expr, visit=visit, collect=collect)

    # Get the expression type; treat iterators like lists
    typ = cast(type, list if isinstance(expr, Iterator) else expr.__class__)

    # do not visit mock objects
    if isinstance(expr, Mock):
        return expr if collect else None

    elif isinstance(expr, no_visit_types):
        result = await visit(expr)
        return result if collect else None

    elif isinstance(expr, (list, tuple, set, Iterator)):
        result = await gather_tasks(*[visit_nested(o) for o in expr])
        return typ(result) if collect else None

    elif isinstance(expr, (dict, OrderedDict)):
        keys, values = zip(*expr.items()) if expr else ([], [])
        keys = await gather_tasks(*[visit_nested(k) for k in keys])
        values = await gather_tasks(*[visit_nested(v) for v in values])
        return typ(zip(keys, values)) if collect else None

    elif is_dataclass(expr) and not isinstance(expr, type):
        values = await gather_tasks(*[visit_nested(getattr(expr, f.name)) for f in fields(expr)])
        result = {field.name: value for field, value in zip(fields(expr), values)}
        return typ(**result) if collect else None

    elif _has_pydantic and isinstance(expr, BaseModel):
        values = await gather_tasks(*[visit_nested(getattr(expr, field)) for field in expr.__fields__])
        result = {field: value for field, value in zip(expr.__fields__, values)}
        return typ(**result) if collect else None

    else:
        result = await visit(expr)
        return result if collect else None


class StreamIterable(io.RawIOBase, IO[bytes]):
    _source: Iterator[bytes]
    _current: bytearray
    _bytes_read: int

    def __init__(self, source: Iterable[bytes]) -> None:
        self._source = iter(source)
        self._current = bytearray()
        self._bytes_read = 0

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._bytes_read

    def readall(self) -> bytes:
        return b"".join(self._source)

    def read(self, n: int = -1) -> bytes:
        if n == -1:
            return self.readall()

        if n < 0:
            raise ValueError("negative read size")

        result = bytearray(n)
        self.readinto(result)
        return bytes(result[:n])

    def readinto(self, target: Union[bytearray, memoryview]) -> int:
        requested = len(target)
        if not isinstance(target, memoryview):
            target = memoryview(target)

        target = target.cast("B")
        if target.nbytes == 0:
            return 0

        while len(self._current) < requested:
            chunk = next(self._source, b"")
            if not chunk:
                break
            self._current.extend(chunk)

        size = min(len(self._current), requested)
        target[:size] = self._current[:size]
        self._current = self._current[size:]
        self._bytes_read += size
        return size

    def iter_chunks(self, chunk_size: int) -> Iterator[bytes]:
        target = bytearray(chunk_size)
        while True:
            size = self.readinto(target)
            if size == 0:
                break
            yield bytes(target[:size])

    def __iter__(self) -> Iterator[bytes]:
        return self.iter_chunks(io.DEFAULT_BUFFER_SIZE)


class AsyncStreamIterable:
    _source: AsyncIterator[bytes]
    _current: bytearray
    _bytes_read: int

    def __init__(self, source: AsyncIterable[bytes]) -> None:
        self._source = aiter(source)
        self._current = bytearray()
        self._bytes_read = 0

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._bytes_read

    async def readall(self) -> bytes:
        data = b"".join([bytes(self._current)] + [x async for x in self._source])
        self._bytes_read += len(data)
        return data

    async def read(self, n: int = -1) -> bytes:
        if n == -1:
            return await self.readall()

        if n < 0:
            raise ValueError("negative read size")

        result = bytearray(n)
        n = await self.readinto(result)
        return bytes(result[:n])

    async def readinto(self, target: Union[bytearray, memoryview]) -> int:
        requested = len(target)
        if not isinstance(target, memoryview):
            target = memoryview(target)

        target = target.cast("B")
        if target.nbytes == 0:
            return 0

        while len(self._current) < requested:
            chunk = await anext(self._source, b"")
            if not chunk:
                break
            self._current.extend(chunk)

        size = min(len(self._current), requested)
        target[:size] = self._current[:size]
        self._current = self._current[size:]
        self._bytes_read += size
        return size

    async def aiter_chunks(self, chunk_size: int) -> AsyncIterator[bytes]:
        target = bytearray(chunk_size)
        while True:
            size = self.readinto(target)
            if size == 0:
                break
            yield bytes(target[:size])

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self.aiter_chunks(io.DEFAULT_BUFFER_SIZE)


class AwaitableValue(Generic[T]):
    __slots__ = ("value",)

    def __init__(self, value: T):
        self.value = value

    def __await__(self) -> Generator[None, None, T]:
        if False:
            yield
        return self.value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value!r})"


class CachedProperty(Generic[T]):
    def __init__(self, getter: Callable[[Any], Awaitable[T]]):
        self.__wrapped__ = getter
        self._name = getter.__name__
        self.__doc__ = getter.__doc__

    def __set_name__(self, owner: Any, name: str) -> None:
        if not any("__dict__" in dir(cls) for cls in owner.__mro__):
            raise TypeError("'cached_property' requires '__dict__' " f"on {owner.__name__!r} to store {name}")
        self._name = name

    @overload
    def __get__(self, instance: None, owner: type) -> "CachedProperty[T]":
        ...

    @overload
    def __get__(self, instance: object, owner: Optional[type]) -> Awaitable[T]:
        ...

    def __get__(self, instance: Optional[object], owner: Optional[type]) -> Union["CachedProperty[T]", Awaitable[T]]:
        if instance is None:
            return self
        return self._get_attribute(instance)

    async def _get_attribute(self, instance: object) -> T:
        value = await self.__wrapped__(instance)
        instance.__dict__[self._name] = AwaitableValue(value)
        return value


cached_property = CachedProperty
