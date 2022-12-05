import asyncio
import sys
from typing import AsyncGenerator, Coroutine, Generator, List, Optional, Union, cast

import pytest

from cbasyncio.aio import (
    TaskGroup,
    TaskInfo,
    TaskStatus,
    get_current_task,
    get_running_tasks,
    move_on_after,
    wait_all_tasks_blocked,
)


@pytest.mark.parametrize(
    "name_input,expected",
    [
        (None, "test_debugging.test_non_main_task_name.<locals>.non_main"),
        (b"name", "b'name'"),
        ("name", "name"),
        ("", ""),
    ],
)
@pytest.mark.asyncio
async def test_non_main_task_name(name_input: Optional[Union[bytes, str]], expected: str) -> None:
    async def non_main(*, task_status: TaskStatus) -> None:
        task_status.started(get_current_task().name)

    name = None
    async with TaskGroup() as tg:
        name = await tg.start(non_main, name=name_input)

    assert name == expected


@pytest.mark.asyncio
async def test_get_running_tasks() -> None:
    async def inspect() -> None:
        await wait_all_tasks_blocked()
        new_tasks = set(get_running_tasks()) - existing_tasks
        task_infos[:] = sorted(new_tasks, key=lambda info: info.name or "")
        event.set()

    event = asyncio.Event()
    task_infos: List[TaskInfo] = []
    host_task = get_current_task()
    async with TaskGroup() as tg:
        existing_tasks = set(get_running_tasks())
        tg.start_soon(event.wait, name="task1")
        tg.start_soon(event.wait, name="task2")
        tg.start_soon(inspect)

    assert len(task_infos) == 3
    expected_names = [
        "task1",
        "task2",
        "test_debugging.test_get_running_tasks.<locals>.inspect",
    ]
    for task, expected_name in zip(task_infos, expected_names):
        assert task.parent_id == host_task.id
        assert task.name == expected_name
        assert repr(task) == f"TaskInfo(id={task.id}, name={expected_name!r})"


@pytest.mark.skipif(
    sys.version_info >= (3, 11),
    reason="Generator based coroutines have been removed in Python 3.11",
)
@pytest.mark.filterwarnings('ignore:"@coroutine" decorator is deprecated:DeprecationWarning')
@pytest.mark.asyncio
def test_wait_generator_based_task_blocked() -> None:
    async def native_coro_part() -> None:
        await wait_all_tasks_blocked()
        gen = cast(Generator, gen_task.get_coro())
        assert not gen.gi_running
        coro = cast(Coroutine, gen.gi_yieldfrom)
        assert coro.cr_code.co_name == "wait"

        event.set()

    @asyncio.coroutine
    def generator_part() -> Generator[object, BaseException, None]:
        yield from event.wait()

    event = asyncio.Event()
    gen_task: asyncio.Task[None] = asyncio.get_event_loop().create_task(generator_part())  # type: ignore[arg-type]
    asyncio.get_event_loop().run_until_complete(native_coro_part())


@pytest.mark.asyncio
async def test_wait_all_tasks_blocked_asend() -> None:
    """Test that wait_all_tasks_blocked() does not crash on an `asend()` object."""

    async def agen_func() -> AsyncGenerator[None, None]:
        yield

    agen = agen_func()
    coro = agen.asend(None)
    loop = asyncio.get_event_loop()
    task: asyncio.Task[None] = loop.create_task(coro)  # type: ignore[arg-type]
    await wait_all_tasks_blocked()
    await task
    await agen.aclose()


@pytest.mark.asyncio
async def test_wait_all_tasks_blocked_cancelled_task() -> None:
    done = False

    async def self_cancel(*, task_status: TaskStatus) -> None:
        nonlocal done
        task_status.started()
        with move_on_after(-1):
            await asyncio.Event().wait()

        done = True

    async with TaskGroup() as tg:
        await tg.start(self_cancel)
        await wait_all_tasks_blocked()
        assert done
