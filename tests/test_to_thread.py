import asyncio
import threading
import time
from concurrent.futures import Future
from contextvars import ContextVar
from functools import partial
from typing import Any, List, NoReturn, Optional

import pytest

from cbasyncio import CapacityLimiter, TaskGroup, to_thread, wait_all_tasks_blocked


@pytest.mark.asyncio
async def test_run_in_thread_cancelled() -> None:
    state = 0

    def thread_worker() -> None:
        nonlocal state
        state = 2

    async def worker() -> None:
        nonlocal state
        state = 1
        await to_thread.run_sync(thread_worker)
        state = 3

    async with TaskGroup() as tg:
        tg.start_soon(worker)
        tg.cancel_scope.cancel()

    assert state == 1


@pytest.mark.asyncio
async def test_run_in_thread_exception() -> None:
    def thread_worker() -> NoReturn:
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        await to_thread.run_sync(thread_worker)

    exc.match("^foo$")


@pytest.mark.asyncio
async def test_run_in_custom_limiter() -> None:
    num_active_threads = max_active_threads = 0

    def thread_worker() -> None:
        nonlocal num_active_threads, max_active_threads
        num_active_threads += 1
        max_active_threads = max(num_active_threads, max_active_threads)
        event.wait(1)
        num_active_threads -= 1

    async def task_worker() -> None:
        await to_thread.run_sync(thread_worker, limiter=limiter)

    event = threading.Event()
    limiter = CapacityLimiter(3)
    async with TaskGroup() as tg:
        for _ in range(4):
            tg.start_soon(task_worker)

        await asyncio.sleep(0.1)
        assert num_active_threads == 3
        assert limiter.borrowed_tokens == 3
        event.set()

    assert num_active_threads == 0
    assert max_active_threads == 3


@pytest.mark.asyncio
async def test_cancel_wait_on_thread() -> None:
    event = threading.Event()
    future: Future[bool] = Future()

    def wait_event() -> None:
        future.set_result(event.wait(1))

    async with TaskGroup() as tg:
        tg.start_soon(partial(to_thread.run_sync, cancellable=True), wait_event)
        await wait_all_tasks_blocked()
        tg.cancel_scope.cancel()

    await to_thread.run_sync(event.set)
    assert future.result(1)


@pytest.mark.asyncio
async def test_contextvar_propagation() -> None:
    var = ContextVar("var", default=1)
    var.set(6)
    assert await to_thread.run_sync(var.get) == 6


@pytest.mark.asyncio
async def test_asyncio_cancel_native_task() -> None:
    task: "Optional[asyncio.Task[None]]" = None

    async def run_in_thread() -> None:
        nonlocal task
        task = asyncio.current_task()
        await to_thread.run_sync(time.sleep, 0.2, cancellable=True)

    async with TaskGroup() as tg:
        tg.start_soon(run_in_thread)
        await wait_all_tasks_blocked()
        assert task is not None
        task.cancel()


def test_asyncio_no_root_task() -> None:
    """
    Regression test for #264.

    Ensures that to_thread.run_sync() does not raise an error when there is no root task, but
    instead tries to find the top most parent task by traversing the cancel scope tree, or failing
    that, uses the current task to set up a shutdown callback.

    """
    evt_loop = asyncio.get_event_loop()

    async def run_in_thread() -> None:
        try:
            await to_thread.run_sync(time.sleep, 0)
        finally:
            evt_loop.call_soon(evt_loop.stop)

    task = evt_loop.create_task(run_in_thread())
    evt_loop.run_forever()
    task.result()

    # Wait for worker threads to exit
    for t in threading.enumerate():
        if t.name == "AIO worker thread":
            t.join(2)
            assert not t.is_alive()


def test_asyncio_future_callback_partial() -> None:
    """
    Regression test for #272.

    Ensures that futures with partial callbacks are handled correctly when the root task
    cannot be determined.
    """
    evt_loop = asyncio.get_event_loop()

    def func(future: object) -> None:
        pass

    async def sleep_sync() -> None:
        return await to_thread.run_sync(time.sleep, 0)

    task = evt_loop.create_task(sleep_sync())
    task.add_done_callback(partial(func))
    evt_loop.run_until_complete(task)


def test_asyncio_run_sync_no_asyncio_run() -> None:
    """Test that the thread pool shutdown callback does not raise an exception."""

    def exception_handler(loop: object, context: Any = None) -> None:
        exceptions.append(context["exception"])

    evt_loop = asyncio.get_event_loop()
    exceptions: List[BaseException] = []
    evt_loop.set_exception_handler(exception_handler)
    evt_loop.run_until_complete(to_thread.run_sync(time.sleep, 0))
    assert not exceptions


def test_asyncio_run_sync_multiple() -> None:
    """Regression test for #304."""
    evt_loop = asyncio.get_event_loop()
    evt_loop.call_later(0.5, evt_loop.stop)
    for _ in range(3):
        evt_loop.run_until_complete(to_thread.run_sync(time.sleep, 0))

    for t in threading.enumerate():
        if t.name == "AIO worker thread":
            t.join(2)
            assert not t.is_alive()


def test_asyncio_no_recycle_stopping_worker() -> None:
    """Regression test for #323."""

    evt_loop = asyncio.get_event_loop()

    async def taskfunc1() -> None:
        await to_thread.run_sync(time.sleep, 0)
        event1.set()
        await event2.wait()

    async def taskfunc2() -> None:
        await event1.wait()
        evt_loop.call_soon(event2.set)
        await to_thread.run_sync(time.sleep, 0)
        # At this point, the worker would be stopped but still in the idle workers pool, so the
        # following would hang prior to the fix
        await to_thread.run_sync(time.sleep, 0)

    event1 = asyncio.Event()
    event2 = asyncio.Event()
    task1 = evt_loop.create_task(taskfunc1())
    task2 = evt_loop.create_task(taskfunc2())
    evt_loop.run_until_complete(asyncio.gather(task1, task2))
