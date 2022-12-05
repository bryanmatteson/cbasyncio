from contextlib import AbstractContextManager
from typing import Any, AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Iterator, Protocol, TypeVar, Union

from typing_extensions import ParamSpec, TypeVarTuple

T = TypeVar("T")
Tco = TypeVar("Tco", covariant=True)
Tcontra = TypeVar("Tcontra", contravariant=True)

T0 = TypeVar("T0")
T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")
T6 = TypeVar("T6")
T7 = TypeVar("T7")
T8 = TypeVar("T8")
T9 = TypeVar("T9")

CM = TypeVar("CM", bound=AbstractContextManager)
A = TypeVar("A")
B = TypeVar("B")
R = TypeVar("R")
Rcontra = TypeVar("Rcontra", contravariant=True)
Rco = TypeVar("Rco", covariant=True)
C = TypeVar("C", bound=Callable)
U = TypeVarTuple("U")
N = TypeVar("N", int, float)
P = ParamSpec("P")
P1 = ParamSpec("P1")

AnyFunction = Union[Callable[..., R], Callable[..., Awaitable[R]]]
AnyTransform = Union[Callable[[T], R], Callable[[T], Awaitable[R]]]
AnyIterable = Union[Iterable[T], AsyncIterable[T]]
AnyIterableIterable = Union[Iterable[AnyIterable[T]], AsyncIterable[AnyIterable[T]]]
AnyIterator = Union[Iterator[T], AsyncIterator[T]]
AnyStop = (StopIteration, StopAsyncIteration)
Accumulator = Union[Callable[[T, T], T], Callable[[T, T], Awaitable[T]]]
KeyFunction = Union[Callable[[T], R], Callable[[T], Awaitable[R]]]
Predicate = Union[Callable[[T], Any], Callable[[T], Awaitable[Any]]]
MaybeAwaitable = Union[T, Awaitable[T]]


LT = TypeVar("LT", bound="SupportsLessThan")


class SupportsLessThan(Protocol):
    def __lt__(self: LT, other: LT) -> bool:
        raise NotImplementedError


ADD = TypeVar("ADD", bound="SupportsAdd")


class SupportsAdd(Protocol):
    def __add__(self: ADD, other: ADD) -> ADD:
        raise NotImplementedError
