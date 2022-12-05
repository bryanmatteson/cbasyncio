import functools
from contextlib import AbstractContextManager
from typing import Any, AsyncIterable, Callable, Generic, Iterable

from typing_extensions import Self

from cbasyncio.types import AsyncIterate, SupportsClose

from . import to_thread
from ._types import CM, P, R, T


class Asyncer(Generic[T]):
    def __init__(self, raw: T) -> None:
        self._raw = raw

    @property
    def raw(self) -> T:
        return self._raw

    async def run_sync(self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        fn = functools.partial(func, *args, **kwargs)
        return await to_thread.run_sync(fn)

    def iterate(self, it: Iterable[R]) -> AsyncIterable[R]:
        return AsyncIterate(it)


class AsyncerContextManager(Asyncer[CM]):
    async def __aenter__(self) -> Self:
        if isinstance(self.raw, AbstractContextManager):
            await to_thread.run_sync(self.raw.__enter__)
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        if isinstance(self.raw, AbstractContextManager):
            await to_thread.run_sync(self.raw.__exit__, *exc_info)

    async def close(self) -> None:
        if isinstance(self.raw, SupportsClose):
            await to_thread.run_sync(self.raw.close)
