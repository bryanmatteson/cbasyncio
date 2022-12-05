import asyncio
from typing import Any, Awaitable, Callable, Dict, List, Set
from uuid import UUID, uuid4

from typing_extensions import Self

from ._types import T
from .aio import TaskGroup


class GatherIncomplete(RuntimeError):
    """Used to indicate retrieving gather results before completion"""


class GatherTaskGroup:
    _task_group: TaskGroup
    _results: Dict[UUID, Any]

    def __init__(self, task_group: TaskGroup):
        self._results = {}
        self._task_group = task_group

    async def _run_and_store(self, key, fn, args):
        self._results[key] = await fn(*args)

    def create_task(self, fn: Callable[..., Any], *args: Any) -> UUID:
        key = uuid4()
        self._results[key] = GatherIncomplete
        self._task_group.start_soon(self._run_and_store, key, fn, args)
        return key

    def get_result(self, key: UUID) -> Any:
        result = self._results[key]
        if result is GatherIncomplete:
            raise GatherIncomplete("Task is not complete. Results should not be retrieved until the task group exits.")
        return result

    async def __aenter__(self) -> Self:
        await self._task_group.__aenter__()
        return self

    async def __aexit__(self, *tb) -> Any:
        try:
            retval = await self._task_group.__aexit__(*tb)
            return retval
        finally:
            del self._task_group


def create_gather_task_group() -> GatherTaskGroup:
    return GatherTaskGroup(TaskGroup())


async def gather_tasks(*calls: Callable[[], Awaitable[T]]) -> List[T]:
    keys = []
    async with create_gather_task_group() as tg:
        for call in calls:
            keys.append(tg.create_task(call))
    return [tg.get_result(key) for key in keys]


async def gather_with_limit(*args: Awaitable[T], return_exceptions: bool = False, limit: int = -1) -> List[T]:
    input_map: Dict[Awaitable[T], List[int]] = {}
    pos: Dict[asyncio.Future[T], int] = {}
    ret: List[Any] = [None] * len(args)

    pending: Set[asyncio.Future[T]] = set()
    done: Set[asyncio.Future[T]] = set()

    next_arg = 0

    while True:
        while next_arg < len(args) and (limit == -1 or len(pending) < limit):
            if args[next_arg] in input_map:
                input_map[args[next_arg]].append(next_arg)
            else:
                task: asyncio.Future[T] = asyncio.ensure_future(args[next_arg])
                pending.add(task)
                pos[task] = next_arg
                input_map[args[next_arg]] = [next_arg]
            next_arg += 1

        if pending:
            try:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for x in done:
                    if return_exceptions and x.exception():
                        ret[pos[x]] = x.exception()
                    else:
                        ret[pos[x]] = x.result()
            except asyncio.CancelledError:
                for x in pending:
                    x.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                raise

        if not pending and next_arg == len(args):
            break

    for lst in input_map.values():
        for i in range(1, len(lst)):
            ret[lst[i]] = ret[lst[0]]

    return ret
