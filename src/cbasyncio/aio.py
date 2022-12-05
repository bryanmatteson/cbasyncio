from __future__ import annotations

import asyncio
import math
import sys
import warnings
from asyncio.base_events import _run_until_complete_cb  # type: ignore[attr-defined]
from dataclasses import dataclass
from inspect import CORO_RUNNING, CORO_SUSPENDED, GEN_RUNNING, GEN_SUSPENDED, getcoroutinestate, getgeneratorstate
from traceback import StackSummary, walk_tb
from types import TracebackType
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    OrderedDict,
    Protocol,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)
from weakref import WeakKeyDictionary

from ._types import R
from .errors import WouldBlock
from .runvars import RunVar


class _Sentinel:
    def __repr__(self):
        return "<implicit>"


_sentinel = _Sentinel()


class TaskStatus(Protocol):
    def started(self, value: object = None) -> None:
        """Signal that the task has started."""


class TaskState:
    __slots__ = "parent_id", "name", "cancel_scope"

    def __init__(
        self, parent_id: Optional[int], name: Optional[str], cancel_scope: Optional[CancelScope],
    ):
        self.parent_id = parent_id
        self.name = name
        self.cancel_scope = cancel_scope


_task_states: WeakKeyDictionary[asyncio.Task, TaskState] = WeakKeyDictionary()


def run(func: Callable[..., Awaitable[R]], *args: object, debug: bool = False, use_uvloop: bool = False,) -> R:
    async def wrapper() -> R:
        task = asyncio.current_task()
        assert task is not None

        task_state = TaskState(None, get_callable_name(func), None)
        _task_states[task] = task_state
        task.set_name(task_state.name)

        try:
            return await func(*args)
        finally:
            del _task_states[task]

    if use_uvloop and sys.implementation.name == "cpython":
        try:
            import uvloop  # type: ignore

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ImportError:
            warnings.warn("uvloop not installed, falling back to default event loop")
    return asyncio.run(wrapper(), debug=debug)


class CancelScope:
    def __init__(self, deadline: float = math.inf, shield: bool = False):
        self._deadline = deadline
        self._shield = shield
        self._parent_scope: Optional[CancelScope] = None
        self._cancel_called = False
        self._active = False
        self._timeout_handle: Optional[asyncio.TimerHandle] = None
        self._cancel_handle: Optional[asyncio.Handle] = None
        self._tasks: Set[asyncio.Task] = set()
        self._host_task: Optional[asyncio.Task] = None
        self._timeout_expired = False

    def __enter__(self) -> "CancelScope":
        if self._active:
            raise RuntimeError("Each CancelScope may only be used for a single 'with' block")

        self._host_task = host_task = cast(asyncio.Task, asyncio.current_task())
        self._tasks.add(host_task)
        try:
            task_state = _task_states[host_task]
        except KeyError:
            task_name = host_task.get_name()
            task_state = TaskState(None, task_name, self)
            _task_states[host_task] = task_state
        else:
            self._parent_scope = task_state.cancel_scope
            task_state.cancel_scope = self

        self._timeout()
        self._active = True
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ):
        if not self._active:
            raise RuntimeError("This cancel scope is not active")
        if asyncio.current_task() is not self._host_task:
            raise RuntimeError("Attempted to exit cancel scope in a different task than it was entered in")

        assert self._host_task is not None
        host_task_state = _task_states.get(self._host_task)
        if host_task_state is None or host_task_state.cancel_scope is not self:
            raise RuntimeError(
                "Attempted to exit a cancel scope that isn't the current tasks's " "current cancel scope"
            )

        self._active = False
        if self._timeout_handle:
            self._timeout_handle.cancel()
            self._timeout_handle = None

        self._tasks.remove(self._host_task)

        host_task_state.cancel_scope = self._parent_scope

        # Restart the cancellation effort in the farthest directly cancelled parent scope if this
        # one was shielded
        if self._shield:
            self._deliver_cancellation_to_parent()

        if exc_val is not None:
            exceptions = exc_val.exceptions if isinstance(exc_val, ExceptionGroup) else [exc_val]
            if all(isinstance(exc, asyncio.CancelledError) for exc in exceptions):
                if self._timeout_expired:
                    return True
                elif not self._cancel_called:
                    # Task was cancelled natively
                    return None
                elif not self._parent_cancelled():
                    # This scope was directly cancelled
                    return True

        return None

    def _timeout(self) -> None:
        if self._deadline != math.inf:
            loop = asyncio.get_running_loop()
            if loop.time() >= self._deadline:
                self._timeout_expired = True
                self.cancel()
            else:
                self._timeout_handle = loop.call_at(self._deadline, self._timeout)

    def _deliver_cancellation(self) -> None:
        """
        Deliver cancellation to directly contained tasks and nested cancel scopes.

        Schedule another run at the end if we still have tasks eligible for cancellation.
        """
        should_retry = False
        current = asyncio.current_task()
        for task in self._tasks:
            if task._must_cancel:  # type: ignore[attr-defined]
                continue

            # The task is eligible for cancellation if it has started and is not in a cancel
            # scope shielded from this one
            cancel_scope = _task_states[task].cancel_scope
            while cancel_scope is not self:
                if cancel_scope is None or cancel_scope._shield:
                    break
                else:
                    cancel_scope = cancel_scope._parent_scope
            else:
                should_retry = True
                if task is not current and (task is self._host_task or _task_started(task)):
                    task.cancel()

        # Schedule another callback if there are still tasks left
        if should_retry:
            self._cancel_handle = asyncio.get_running_loop().call_soon(self._deliver_cancellation)
        else:
            self._cancel_handle = None

    def _deliver_cancellation_to_parent(self) -> None:
        """Start cancellation effort in the farthest directly cancelled parent scope"""
        scope = self._parent_scope
        scope_to_cancel: Optional[CancelScope] = None
        while scope is not None:
            if scope._cancel_called and scope._cancel_handle is None:
                scope_to_cancel = scope

            # No point in looking beyond any shielded scope
            if scope._shield:
                break

            scope = scope._parent_scope

        if scope_to_cancel is not None:
            scope_to_cancel._deliver_cancellation()

    def _parent_cancelled(self) -> bool:
        # Check whether any parent has been cancelled
        cancel_scope = self._parent_scope
        while cancel_scope is not None and not cancel_scope._shield:
            if cancel_scope._cancel_called:
                return True
            else:
                cancel_scope = cancel_scope._parent_scope

        return False

    def cancel(self) -> None:
        if not self._cancel_called:
            if self._timeout_handle:
                self._timeout_handle.cancel()
                self._timeout_handle = None

            self._cancel_called = True
            self._deliver_cancellation()

    @property
    def deadline(self) -> float:
        return self._deadline

    @deadline.setter
    def deadline(self, value: float) -> None:
        self._deadline = float(value)
        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
            self._timeout_handle = None

        if self._active and not self._cancel_called:
            self._timeout()

    @property
    def cancel_called(self) -> bool:
        return self._cancel_called

    @property
    def shield(self) -> bool:
        return self._shield

    @shield.setter
    def shield(self, value: bool) -> None:
        if self._shield != value:
            self._shield = value
            if not value:
                self._deliver_cancellation_to_parent()


def _task_started(task: asyncio.Task) -> bool:
    """Return ``True`` if the task has been started and has not finished."""
    coro = cast(Coroutine[Any, Any, Any], task.get_coro())
    try:
        return getcoroutinestate(coro) in (CORO_RUNNING, CORO_SUSPENDED)
    except AttributeError:
        try:
            return getgeneratorstate(cast(Generator, coro)) in (GEN_RUNNING, GEN_SUSPENDED,)
        except AttributeError:
            raise Exception(f"Cannot determine if task {task} has started or not")


async def checkpoint_if_cancelled() -> None:
    task = asyncio.current_task()
    if task is None:
        return

    try:
        cancel_scope = _task_states[task].cancel_scope
    except KeyError:
        return

    while cancel_scope:
        if cancel_scope.cancel_called:
            await asyncio.sleep(0)
        elif cancel_scope.shield:
            break
        else:
            cancel_scope = cancel_scope._parent_scope


async def cancel_shielded_checkpoint() -> None:
    with CancelScope(shield=True):
        await asyncio.sleep(0)


def current_effective_deadline() -> float:
    try:
        cancel_scope = _task_states[asyncio.current_task()].cancel_scope  # type: ignore[index]
    except KeyError:
        return math.inf

    deadline = math.inf
    while cancel_scope:
        deadline = min(deadline, cancel_scope.deadline)
        if cancel_scope.shield:
            break
        else:
            cancel_scope = cancel_scope._parent_scope

    return deadline


class FailAfterContextManager:
    def __init__(self, cancel_scope: CancelScope):
        self._cancel_scope = cancel_scope

    def __enter__(self) -> CancelScope:
        return self._cancel_scope.__enter__()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        retval = self._cancel_scope.__exit__(exc_type, exc_val, exc_tb)
        if self._cancel_scope.cancel_called:
            raise TimeoutError

        return retval


def fail_after(delay: Optional[float], shield: bool = False) -> FailAfterContextManager:
    deadline = (asyncio.get_running_loop().time() + delay) if delay is not None else math.inf
    cancel_scope = CancelScope(deadline=deadline, shield=shield)
    return FailAfterContextManager(cancel_scope)


def move_on_after(delay: Optional[float], shield: bool = False) -> CancelScope:
    deadline = (asyncio.get_running_loop().time() + delay) if delay is not None else math.inf
    return CancelScope(deadline=deadline, shield=shield)


class _AsyncioTaskStatus:
    def __init__(self, future: asyncio.Future, parent_id: int):
        self._future = future
        self._parent_id = parent_id

    def started(self, value: object = None) -> None:
        try:
            self._future.set_result(value)
        except asyncio.InvalidStateError:
            raise RuntimeError("called 'started' twice on the same task status") from None

        task = cast(asyncio.Task, asyncio.current_task())
        _task_states[task].parent_id = self._parent_id


class TaskGroup:
    def __init__(self) -> None:
        self.cancel_scope: CancelScope = CancelScope()
        self._active = False
        self._exceptions: List[BaseException] = []

    async def __aenter__(self) -> "TaskGroup":
        self.cancel_scope.__enter__()
        self._active = True
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ):
        ignore_exception = self.cancel_scope.__exit__(exc_type, exc_val, exc_tb)
        if exc_val is not None:
            self.cancel_scope.cancel()
            self._exceptions.append(exc_val)

        while self.cancel_scope._tasks:
            try:
                await asyncio.wait(self.cancel_scope._tasks)
            except asyncio.CancelledError:
                self.cancel_scope.cancel()

        self._active = False
        if not self.cancel_scope._parent_cancelled():
            exceptions = self._filter_cancellation_errors(self._exceptions)
        else:
            exceptions = self._exceptions

        try:
            if len(exceptions) > 1:
                if all(isinstance(e, asyncio.CancelledError) and not e.args for e in exceptions):
                    # Tasks were cancelled natively, without a cancellation message
                    raise asyncio.CancelledError
                else:
                    raise ExceptionGroup(exceptions)
            elif exceptions and exceptions[0] is not exc_val:
                raise exceptions[0]
        except BaseException as exc:
            # Clear the context here, as it can only be done in-flight.
            # If the context is not cleared, it can result in recursive tracebacks (see #145).
            exc.__context__ = None
            raise

        return ignore_exception

    @staticmethod
    def _filter_cancellation_errors(exceptions: Sequence[BaseException],) -> List[BaseException]:
        filtered_exceptions: List[BaseException] = []
        for exc in exceptions:
            if isinstance(exc, ExceptionGroup):
                new_exceptions = TaskGroup._filter_cancellation_errors(exc.exceptions)
                if len(new_exceptions) > 1:
                    filtered_exceptions.append(exc)
                elif len(new_exceptions) == 1:
                    filtered_exceptions.append(new_exceptions[0])
                elif new_exceptions:
                    new_exc = ExceptionGroup(new_exceptions)
                    new_exc.__cause__ = exc.__cause__
                    new_exc.__context__ = exc.__context__
                    new_exc.__traceback__ = exc.__traceback__
                    filtered_exceptions.append(new_exc)
            elif not isinstance(exc, asyncio.CancelledError) or exc.args:
                filtered_exceptions.append(exc)

        return filtered_exceptions

    def _spawn(
        self,
        func: Callable[..., Coroutine],
        args: tuple,
        name: object,
        task_status_future: Optional[asyncio.Future] = None,
    ) -> asyncio.Task:
        def task_done(_task: asyncio.Task) -> None:
            # This is the code path for Python 3.8+
            assert _task in self.cancel_scope._tasks
            self.cancel_scope._tasks.remove(_task)
            del _task_states[_task]

            try:
                exc = _task.exception()
            except asyncio.CancelledError as e:
                while isinstance(e.__context__, asyncio.CancelledError):
                    e = e.__context__

                exc = e

            if exc is not None:
                if task_status_future is None or task_status_future.done():
                    self._exceptions.append(exc)
                    self.cancel_scope.cancel()
                else:
                    task_status_future.set_exception(exc)
            elif task_status_future is not None and not task_status_future.done():
                task_status_future.set_exception(RuntimeError("Child exited without calling task_status.started()"))

        if not self._active:
            raise RuntimeError("This task group is not active; no new tasks can be started.")

        options = {}
        name = get_callable_name(func) if name is None else str(name)
        options["name"] = name

        kwargs = {}
        if task_status_future:
            parent_id = id(asyncio.current_task())
            kwargs["task_status"] = _AsyncioTaskStatus(task_status_future, id(self.cancel_scope._host_task))
        else:
            parent_id = id(self.cancel_scope._host_task)

        coro = func(*args, **kwargs)
        if not asyncio.iscoroutine(coro):
            raise TypeError(f"Expected an async function, but {func} appears to be synchronous")

        task = asyncio.create_task(coro, **options)
        task.add_done_callback(task_done)

        # Make the spawned task inherit the task group's cancel scope
        _task_states[task] = TaskState(parent_id=parent_id, name=name, cancel_scope=self.cancel_scope)
        self.cancel_scope._tasks.add(task)
        return task

    def start_soon(self, func: Callable[..., Coroutine], *args: object, name: object = None) -> asyncio.Task:
        return self._spawn(func, args, name)

    async def start(self, func: Callable[..., Coroutine], *args: object, name: object = None) -> None:
        future: asyncio.Future = asyncio.Future()
        task = self._spawn(func, args, name, future)

        with CancelScope(shield=True):
            try:
                return await future
            except asyncio.CancelledError:
                task.cancel()
                raise


class ExceptionGroup(BaseException):
    def __init__(self, exceptions: List[BaseException]):
        super().__init__()
        self.exceptions = exceptions

    SEPARATOR = "----------------------------\n"

    exceptions: List[BaseException]

    def __str__(self) -> str:
        tracebacks = ["".join(format_exception(type(exc), exc, exc.__traceback__)) for exc in self.exceptions]
        return (
            f"{len(self.exceptions)} exceptions were raised in the task group:\n"
            f"{self.SEPARATOR}{self.SEPARATOR.join(tracebacks)}"
        )

    def __repr__(self) -> str:
        exception_reprs = ", ".join(repr(exc) for exc in self.exceptions)
        return f"<{self.__class__.__name__}: {exception_reprs}>"


def format_exception(
    exc: Type[BaseException],
    /,
    value: Union[BaseException, _Sentinel, None] = _sentinel,
    tb: Union[TracebackType, _Sentinel, None] = _sentinel,
    limit: Optional[int] = None,
    chain: bool = True,
):
    v, tb = _parse_value_tb(exc, value, tb)
    te = TracebackException(type(v), v, tb, limit=limit)  # type: ignore
    return list(te.format(chain=chain))


def _parse_value_tb(
    exc: Type[BaseException], value: Union[BaseException, _Sentinel, None], tb: Union[TracebackType, _Sentinel, None]
) -> Tuple[Union[Type[BaseException], BaseException, None], Optional[TracebackType]]:
    if (value is _sentinel) != (tb is _sentinel):
        raise ValueError("Both or neither of value and tb must be given")

    if value is tb is _sentinel:
        if exc is not None:
            return exc, exc.__traceback__
        else:
            return None, None
    return cast(BaseException, value), cast(Optional[TracebackType], tb)


def get_callable_name(func: Callable) -> str:
    module = getattr(func, "__module__", None)
    qualname = getattr(func, "__qualname__", None)
    return ".".join([x for x in (module, qualname) if x])


_root_task: RunVar[Optional[asyncio.Task]] = RunVar("_root_task")


def find_root_task() -> asyncio.Task:
    root_task = _root_task.get(None)
    if root_task is not None and not root_task.done():
        return root_task

    # Look for a task that has been started via run_until_complete()
    for task in asyncio.all_tasks():
        if task._callbacks and not task.done():
            for cb in _get_task_callbacks(task):
                if cb is _run_until_complete_cb or getattr(cb, "__module__", None) == "uvloop.loop":
                    _root_task.set(task)
                    return task

    # Look up the topmost task in the task tree, if possible
    task = cast(asyncio.Task, asyncio.current_task())
    state = _task_states.get(task)
    if state:
        cancel_scope = state.cancel_scope
        while cancel_scope and cancel_scope._parent_scope is not None:
            cancel_scope = cancel_scope._parent_scope

        if cancel_scope is not None:
            return cast(asyncio.Task, cancel_scope._host_task)

    return task


def _get_task_callbacks(task: asyncio.Task) -> Iterable[Callable]:
    return [cb for cb, context in task._callbacks]


_default_thread_limiter: RunVar[CapacityLimiter] = RunVar("_default_thread_limiter")


def current_default_thread_limiter() -> CapacityLimiter:
    try:
        return _default_thread_limiter.get()
    except LookupError:
        limiter = CapacityLimiter(40)
        _default_thread_limiter.set(limiter)
        return limiter


@dataclass(frozen=True)
class CapacityLimiterStatistics:
    borrowed_tokens: int
    total_tokens: float
    borrowers: Tuple[object, ...]
    tasks_waiting: int


class CapacityLimiter:
    _total_tokens: float = 0

    def __new__(cls, total_tokens: float) -> CapacityLimiter:
        return object.__new__(cls)

    def __init__(self, total_tokens: float):
        self._borrowers: Set[Any] = set()
        self._wait_queue: Dict[Any, asyncio.Event] = OrderedDict()
        self.total_tokens = total_tokens

    async def __aenter__(self) -> None:
        await self.acquire()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ):
        self.release()

    @property
    def total_tokens(self) -> float:
        return self._total_tokens

    @total_tokens.setter
    def total_tokens(self, value: float) -> None:
        if not isinstance(value, int) and not math.isinf(value):
            raise TypeError("total_tokens must be an int or math.inf")
        if value < 1:
            raise ValueError("total_tokens must be >= 1")

        old_value = self._total_tokens
        self._total_tokens = value
        events = []
        for event in self._wait_queue.values():
            if value <= old_value:
                break

            if not event.is_set():
                events.append(event)
                old_value += 1

        for event in events:
            event.set()

    @property
    def borrowed_tokens(self) -> int:
        return len(self._borrowers)

    @property
    def available_tokens(self) -> float:
        return self._total_tokens - len(self._borrowers)

    def acquire_nowait(self) -> None:
        self.acquire_on_behalf_of_nowait(asyncio.current_task())

    def acquire_on_behalf_of_nowait(self, borrower: object) -> None:
        if borrower in self._borrowers:
            raise RuntimeError("this borrower is already holding one of this CapacityLimiter's " "tokens")

        if self._wait_queue or len(self._borrowers) >= self._total_tokens:
            raise WouldBlock

        self._borrowers.add(borrower)

    async def acquire(self) -> None:
        return await self.acquire_on_behalf_of(asyncio.current_task())

    async def acquire_on_behalf_of(self, borrower: object) -> None:
        await checkpoint_if_cancelled()
        try:
            self.acquire_on_behalf_of_nowait(borrower)
        except WouldBlock:
            event = asyncio.Event()
            self._wait_queue[borrower] = event
            try:
                await event.wait()
            except BaseException:
                self._wait_queue.pop(borrower, None)
                raise

            self._borrowers.add(borrower)
        else:
            try:
                await cancel_shielded_checkpoint()
            except BaseException:
                self.release()
                raise

    def release(self) -> None:
        self.release_on_behalf_of(asyncio.current_task())

    def release_on_behalf_of(self, borrower: object) -> None:
        try:
            self._borrowers.remove(borrower)
        except KeyError:
            raise RuntimeError("this borrower isn't holding any of this CapacityLimiter's " "tokens") from None

        # Notify the next task in line if this limiter has free capacity now
        if self._wait_queue and len(self._borrowers) < self._total_tokens:
            event = self._wait_queue.popitem()[1]
            event.set()

    def statistics(self) -> CapacityLimiterStatistics:
        return CapacityLimiterStatistics(
            self.borrowed_tokens, self.total_tokens, tuple(self._borrowers), len(self._wait_queue),
        )


class TaskInfo:
    __slots__ = "_name", "id", "parent_id", "name", "coro"

    def __init__(
        self, id: int, parent_id: Optional[int], name: Optional[str], coro: Union[Generator, Awaitable[Any]],
    ):
        func = get_current_task
        self._name = f"{func.__module__}.{func.__qualname__}"
        self.id: int = id
        self.parent_id: Optional[int] = parent_id
        self.name: Optional[str] = name
        self.coro: Union[Generator, Awaitable[Any]] = coro

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TaskInfo):
            return self.id == other.id

        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.id)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id!r}, name={self.name!r})"

    def _unwrap(self) -> "TaskInfo":
        return self


def _create_task_info(task: asyncio.Task) -> TaskInfo:
    task_state = _task_states.get(task)
    if task_state is None:
        name = task.get_name()
        parent_id = None
    else:
        name = task_state.name
        parent_id = task_state.parent_id

    return TaskInfo(id(task), parent_id, name, task.get_coro())


def get_current_task() -> TaskInfo:
    return _create_task_info(asyncio.current_task())  # type: ignore[arg-type]


def get_running_tasks() -> List[TaskInfo]:
    return [_create_task_info(task) for task in asyncio.all_tasks() if not task.done()]


async def wait_all_tasks_blocked() -> None:
    await asyncio.sleep(0)
    this_task = asyncio.current_task()
    while True:
        for task in asyncio.all_tasks():
            if task is this_task:
                continue

            if task._fut_waiter is None or task._fut_waiter.done():  # type: ignore[attr-defined]
                await asyncio.sleep(0.1)
                break
        else:
            return


def _format_final_exc_line(etype, value):
    valuestr = _some_str(value)
    if value is None or not valuestr:
        line = "%s\n" % etype
    else:
        line = "%s: %s\n" % (etype, valuestr)
    return line


def _some_str(value):
    try:
        return str(value)
    except BaseException:
        return "<unprintable %s object>" % type(value).__name__


class TracebackException:
    __cause__: Any
    __context__: Any

    def __init__(
        self,
        exc_type,
        exc_value,
        exc_traceback,
        *,
        limit=None,
        lookup_lines=True,
        capture_locals=False,
        compact=False,
        _seen=None,
    ):
        is_recursive_call = _seen is not None
        if _seen is None:
            _seen = set()
        _seen.add(id(exc_value))

        self.stack = StackSummary.extract(
            walk_tb(exc_traceback), limit=limit, lookup_lines=lookup_lines, capture_locals=capture_locals
        )
        self.exc_type = exc_type
        self._str = _some_str(exc_value)
        if exc_type and issubclass(exc_type, SyntaxError):
            self.filename = exc_value.filename
            lno = exc_value.lineno
            self.lineno = str(lno) if lno is not None else None
            end_lno = exc_value.end_lineno
            self.end_lineno = str(end_lno) if end_lno is not None else None
            self.text = exc_value.text
            self.offset = exc_value.offset
            self.end_offset = exc_value.end_offset
            self.msg = exc_value.msg
        if lookup_lines:
            self._load_lines()
        self.__suppress_context__ = exc_value.__suppress_context__ if exc_value is not None else False

        if not is_recursive_call:
            queue = [(self, exc_value)]
            while queue:
                te, e = queue.pop()
                if e and e.__cause__ is not None and id(e.__cause__) not in _seen:
                    cause = TracebackException(
                        type(e.__cause__),
                        e.__cause__,
                        e.__cause__.__traceback__,
                        limit=limit,
                        lookup_lines=lookup_lines,
                        capture_locals=capture_locals,
                        _seen=_seen,
                    )
                else:
                    cause = None

                if compact:
                    need_context = cause is None and e is not None and not e.__suppress_context__
                else:
                    need_context = True
                if e and e.__context__ is not None and need_context and id(e.__context__) not in _seen:
                    context = TracebackException(
                        type(e.__context__),
                        e.__context__,
                        e.__context__.__traceback__,
                        limit=limit,
                        lookup_lines=lookup_lines,
                        capture_locals=capture_locals,
                        _seen=_seen,
                    )
                else:
                    context = None
                te.__cause__ = cause
                te.__context__ = context
                if cause:
                    queue.append((te.__cause__, e.__cause__))
                if context:
                    queue.append((te.__context__, e.__context__))

    @classmethod
    def from_exception(cls, exc, *args, **kwargs):
        """Create a TracebackException from an exception."""
        return cls(type(exc), exc, exc.__traceback__, *args, **kwargs)

    def _load_lines(self):
        """Private API. force all lines in the stack to be loaded."""
        for frame in self.stack:
            frame.line

    def __eq__(self, other):
        if isinstance(other, TracebackException):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __str__(self):
        return self._str

    def format_exception_only(self):
        if self.exc_type is None:
            yield _format_final_exc_line(None, self._str)
            return

        stype = self.exc_type.__qualname__
        smod = self.exc_type.__module__
        if smod not in ("__main__", "builtins"):
            if not isinstance(smod, str):
                smod = "<unknown>"
            stype = smod + "." + stype

        if not issubclass(self.exc_type, SyntaxError):
            yield _format_final_exc_line(stype, self._str)
        else:
            yield from self._format_syntax_error(stype)

    def _format_syntax_error(self, stype):
        """Format SyntaxError exceptions (internal helper)."""
        # Show exactly where the problem was found.
        filename_suffix = ""
        if self.lineno is not None:
            yield '  File "{}", line {}\n'.format(self.filename or "<string>", self.lineno)
        elif self.filename is not None:
            filename_suffix = " ({})".format(self.filename)

        text = self.text
        if text is not None:
            # text  = "   foo\n"
            # rtext = "   foo"
            # ltext =    "foo"
            rtext = text.rstrip("\n")
            ltext = rtext.lstrip(" \n\f")
            spaces = len(rtext) - len(ltext)
            yield "    {}\n".format(ltext)

            if self.offset is not None:
                offset = self.offset
                end_offset = self.end_offset if self.end_offset not in {None, 0} else offset
                if offset == end_offset or end_offset == -1:
                    end_offset = offset + 1

                # Convert 1-based column offset to 0-based index into stripped text
                colno = offset - 1 - spaces
                end_colno = end_offset - 1 - spaces
                if colno >= 0:
                    # non-space whitespace (likes tabs) must be kept for alignment
                    caretspace = ((c if c.isspace() else " ") for c in ltext[:colno])
                    yield "    {}{}".format("".join(caretspace), ("^" * (end_colno - colno) + "\n"))
        msg = self.msg or "<no detail available>"
        yield "{}: {}{}\n".format(stype, msg, filename_suffix)

    def format(self, *, chain=True):
        output = []
        exc = self
        while exc:
            if chain:
                if exc.__cause__ is not None:
                    chained_msg = _cause_message
                    chained_exc = exc.__cause__
                elif exc.__context__ is not None and not exc.__suppress_context__:
                    chained_msg = _context_message
                    chained_exc = exc.__context__
                else:
                    chained_msg = None
                    chained_exc = None

                output.append((chained_msg, exc))
                exc = chained_exc
            else:
                output.append((None, exc))
                exc = None

        for msg, exc in reversed(output):
            if msg is not None:
                yield msg
            if exc.stack:
                yield "Traceback (most recent call last):\n"
                yield from exc.stack.format()
            yield from exc.format_exception_only()


_cause_message = "\nThe above exception was the direct cause " "of the following exception:\n\n"
_context_message = "\nDuring handling of the above exception, " "another exception occurred:\n\n"
