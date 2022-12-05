from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import functools
import threading
from collections import deque
from concurrent.futures import Future
from queue import Queue
from typing import Any, Callable, Coroutine, Deque, Optional, Set, Tuple, Union, cast

from ._types import R, T
from .aio import CancelScope, CapacityLimiter, current_default_thread_limiter, find_root_task
from .runvars import RunVar


async def spawn(func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    loop = asyncio.events.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return cast(T, await loop.run_in_executor(None, func_call))


threadlocals = threading.local()

_Retval_Queue_Type = Tuple[Optional[R], Optional[BaseException]]


class WorkerThread(threading.Thread):
    MAX_IDLE_TIME = 10  # seconds

    def __init__(
        self, root_task: asyncio.Task, workers: Set[WorkerThread], idle_workers: Deque[WorkerThread],
    ):
        super().__init__(name="AIO worker thread")
        self.root_task = root_task
        self.workers = workers
        self.idle_workers = idle_workers
        self.loop = root_task._loop
        self.queue: Queue[Union[Tuple[contextvars.Context, Callable, tuple, asyncio.Future], None]] = Queue(2)
        self.idle_since = asyncio.get_running_loop().time()
        self.stopping = False

    def _report_result(self, future: asyncio.Future, result: Any, exc: Optional[BaseException]) -> None:
        self.idle_since = asyncio.get_running_loop().time()
        if not self.stopping:
            self.idle_workers.append(self)

        if not future.cancelled():
            if exc is not None:
                future.set_exception(exc)
            else:
                future.set_result(result)

    def run(self) -> None:
        threadlocals.loop = self.loop
        while True:
            item = self.queue.get()
            if item is None:
                # Shutdown command received
                return

            context, func, args, future = item
            if not future.cancelled():
                result = None
                exception: Optional[BaseException] = None
                try:
                    result = context.run(func, *args)
                except BaseException as exc:
                    exception = exc

                if not self.loop.is_closed():
                    self.loop.call_soon_threadsafe(self._report_result, future, result, exception)

            self.queue.task_done()

    def stop(self, f: Optional[asyncio.Task] = None) -> None:
        self.stopping = True
        self.queue.put_nowait(None)
        self.workers.discard(self)
        try:
            self.idle_workers.remove(self)
        except ValueError:
            pass


_threadpool_idle_workers: RunVar[Deque[WorkerThread]] = RunVar("_threadpool_idle_workers")
_threadpool_workers: RunVar[Set[WorkerThread]] = RunVar("_threadpool_workers")


async def run_sync(
    func: Callable[..., R], *args: object, cancellable: bool = False, limiter: Optional["CapacityLimiter"] = None,
) -> R:
    await asyncio.sleep(0)

    # If this is the first run in this event loop thread, set up the necessary variables
    try:
        idle_workers = _threadpool_idle_workers.get()
        workers = _threadpool_workers.get()
    except LookupError:
        idle_workers = deque()
        workers = set()
        _threadpool_idle_workers.set(idle_workers)
        _threadpool_workers.set(workers)

    async with (limiter or current_default_thread_limiter()):
        with CancelScope(shield=not cancellable):
            future: asyncio.Future = asyncio.Future()
            root_task = find_root_task()
            if not idle_workers:
                worker = WorkerThread(root_task, workers, idle_workers)
                worker.start()
                workers.add(worker)
                root_task.add_done_callback(worker.stop)
            else:
                worker = idle_workers.pop()

                # Prune any other workers that have been idle for MAX_IDLE_TIME seconds or longer
                now = asyncio.get_running_loop().time()
                while idle_workers:
                    if now - idle_workers[0].idle_since < WorkerThread.MAX_IDLE_TIME:
                        break

                    expired_worker = idle_workers.popleft()
                    expired_worker.root_task.remove_done_callback(expired_worker.stop)
                    expired_worker.stop()

            context = contextvars.copy_context()
            worker.queue.put_nowait((context, func, args, future))
            return await future


def run_sync_from_thread(func: Callable[..., R], *args: object, loop: Optional[asyncio.AbstractEventLoop] = None,) -> R:
    @functools.wraps(func)
    def wrapper() -> None:
        try:
            f.set_result(func(*args))
        except BaseException as exc:
            f.set_exception(exc)
            if not isinstance(exc, Exception):
                raise

    f: concurrent.futures.Future[R] = Future()
    loop = loop or threadlocals.loop
    assert loop is not None
    loop.call_soon_threadsafe(wrapper)

    return f.result()


def run_async_from_thread(func: Callable[..., Coroutine[Any, Any, R]], *args: object) -> R:
    f: concurrent.futures.Future[R] = asyncio.run_coroutine_threadsafe(func(*args), threadlocals.loop)
    return f.result()
