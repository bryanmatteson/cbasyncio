from abc import ABCMeta, abstractmethod
from types import TracebackType
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Generic,
    Iterable,
    Iterator,
    Optional,
    Protocol,
    Type,
    runtime_checkable,
)

from typing_extensions import Unpack

from . import to_thread
from ._types import R, T, U


@runtime_checkable
class SupportsClose(Protocol):
    def close(self) -> Any:
        ...


@runtime_checkable
class SupportsAClose(Protocol):
    async def aclose(self) -> Any:
        ...


@runtime_checkable
class SupportsAsyncClose(Protocol):
    async def close(self) -> Any:
        ...


class AsyncResource(metaclass=ABCMeta):
    async def __aenter__(self: T) -> T:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.aclose()

    @abstractmethod
    async def aclose(self) -> None:
        """Close the resource."""


class F(Generic[Unpack[U]]):
    def __new__(cls, fn: Callable[[Unpack[U]], R]) -> Callable[[Unpack[U]], R]:
        return fn


_sentinel = object()


class AsyncIterate(AsyncIterator[T]):
    it: Iterator[T]

    def __init__(self, iterable: Iterable[T]) -> None:
        self.it = iter(iterable)

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self):
        nextval = await to_thread.run_sync(next, self.it, _sentinel, cancellable=True)
        if nextval is _sentinel:
            raise StopAsyncIteration from None

        return nextval


class Sentinel:
    __slots__ = ("name",)

    def __init__(self, name: str):
        self.name = name

    def __repr__(self) -> str:
        return self.name


Unset = Sentinel("Unset")


class NoLock:
    async def __aenter__(self) -> None:
        pass

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        return False
