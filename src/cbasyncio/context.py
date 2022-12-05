import asyncio
import contextlib
import warnings
from types import TracebackType
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Generic,
    Iterable,
    List,
    Optional,
    Protocol,
    Type,
    runtime_checkable,
)

from typing_extensions import Self

from ._types import AnyIterable, T
from .utils import anext, anyiter, is_async_iterator


@runtime_checkable
class SupportsAClose(Protocol):
    async def aclose(self) -> Any:
        ...


@runtime_checkable
class SupportsAsyncClose(Protocol):
    async def close(self) -> Any:
        ...


class aclosing(Generic[T]):
    def __init__(self, thing: T):
        if not isinstance(thing, SupportsAClose):
            raise TypeError(f"{thing} does not support aclose")
        self.thing = thing

    async def __aenter__(self) -> T:
        return self.thing

    async def __aexit__(self, *args: Any) -> None:
        await self.thing.aclose()


class closing_async(Generic[T]):
    def __init__(self, thing: T):
        if not isinstance(thing, SupportsAsyncClose):
            raise TypeError(f"{thing} does not support aclose")
        self.thing = thing

    async def __aenter__(self) -> T:
        return self.thing

    async def __aexit__(self, *args: Any) -> None:
        await self.thing.close()


class AsyncContextGroup:
    def __init__(
        self, context_managers: Optional[Iterable[contextlib.AbstractAsyncContextManager[Any]]] = None,
    ) -> None:
        self._cm = list(context_managers) if context_managers else []
        self._cm_yields: List[Any] = []
        self._cm_exits: List[Any] = []

    def add(self, cm: contextlib.AbstractAsyncContextManager[Any]) -> None:
        self._cm.append(cm)

    async def __aenter__(self) -> List[asyncio.Task]:
        self._cm_yields[:] = await asyncio.gather(*(e.__aenter__() for e in self._cm), return_exceptions=True)
        return self._cm_yields

    async def __aexit__(
        self,
        excType: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        excTb: Optional[TracebackType],
        /,
    ) -> Optional[bool]:
        self._cm_yields.clear()
        self._cm_exits[:] = await asyncio.gather(
            *(e.__aexit__(excType, exc_value, excTb) for e in self._cm), return_exceptions=True
        )

    def exit_states(self) -> List[Any]:
        return self._cm_exits


class AsyncIteratorContext(AsyncIterator[T]):
    _STANDBY = "STANDBY"
    _RUNNING = "RUNNING"
    _FINISHED = "FINISHED"

    def __init__(self, it: AsyncIterator[T]):
        assert is_async_iterator(it)
        if isinstance(it, AsyncIteratorContext):
            raise TypeError(f"{it!r} is already an AsyncIteratorContext")

        self._state = self._STANDBY
        self._it = it

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        if self._state == self._FINISHED:
            raise RuntimeError(f"{type(self).__name__} is closed and cannot be iterated")

        if self._state == self._STANDBY:
            warnings.warn(f"{type(self).__name__} is iterated outside of its context", stacklevel=2)
        return await anext(self._it)

    async def __aenter__(self) -> Self:
        if self._state == self._RUNNING:
            raise RuntimeError(f"{type(self).__name__} has already been entered")

        if self._state == self._FINISHED:
            raise RuntimeError(f"{type(self).__name__} is closed and cannot be iterated")

        self._state = self._RUNNING
        return self

    async def __aexit__(
        self,
        typ: Optional[Type[BaseException]] = None,
        value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ):
        try:
            if self._state == self._FINISHED:
                return False
            try:
                if typ is None:
                    return False

                if typ is GeneratorExit:
                    return False

                if not hasattr(self._it, "athrow"):
                    return False

                if not getattr(self._it, "ag_frame", True):
                    return False

                if getattr(self._it, "ag_running", False):
                    return False

                try:
                    if isinstance(self._it, AsyncGenerator):
                        await self._it.athrow(typ, value, traceback)
                        raise RuntimeError("Async iterator didn't stop after athrow()")

                except StopAsyncIteration as exc:
                    return exc is not value

                except BaseException as exc:
                    if exc is value:
                        return False
                    raise
            finally:
                aclose = getattr(self._it, "aclose", None)

                running = getattr(self._it, "ag_running", False)
                closed = not getattr(self._it, "ag_frame", True)

                if callable(aclose) and not running and not closed:
                    with contextlib.suppress(GeneratorExit):
                        await aclose()
        finally:
            self._state = self._FINISHED

    async def aclose(self):
        await self.__aexit__(None, None, None)

    async def athrow(self, exc):
        if self._state == self._FINISHED:
            raise RuntimeError(f"{type(self).__name__} is closed and cannot be used")
        if athrow := getattr(self._it, "athrow", None):
            if callable(athrow):
                return await athrow(exc)


def aitercontext(it: AnyIterable[T]) -> AsyncIteratorContext[T]:
    it = anyiter(it)
    return it if isinstance(it, AsyncIteratorContext) else AsyncIteratorContext(it)
