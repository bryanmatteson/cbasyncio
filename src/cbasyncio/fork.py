import asyncio
import errno
import logging
import os
import signal
from abc import ABCMeta, abstractmethod
from typing import Callable, Tuple

logger = logging.getLogger(__name__)


_has_pidfd = False
if hasattr(os, "pidfd_open"):
    try:
        os.pidfd_open(0, 0)  # type: ignore
    except OSError as e:
        if e.errno in (errno.EBADF, errno.EINVAL):
            _has_pidfd = True


class AbstractChildProcess(metaclass=ABCMeta):
    @abstractmethod
    def send_signal(self, signum: int) -> None:
        raise NotImplementedError

    @abstractmethod
    async def wait(self) -> int:
        raise NotImplementedError


class PosixChildProcess(AbstractChildProcess):
    def __init__(self, pid: int) -> None:
        self._pid = pid
        self._terminated = False

    def send_signal(self, signum: int) -> None:
        if self._terminated:
            if signum != signal.SIGKILL:
                logger.warning(
                    "PosixChildProcess(%d).send_signal(%d): " "The process has already terminated.", self._pid, signum,
                )
            return
        if signum == signal.SIGKILL:
            logger.warning("Force-killed hanging child: %d", self._pid)
        os.kill(self._pid, signum)

    async def wait(self) -> int:
        loop = asyncio.get_running_loop()
        try:
            _, status = await loop.run_in_executor(None, os.waitpid, self._pid, 0)
        except ChildProcessError:
            self._returncode = 255
            logger.warning("child process pid %d exit status already read: " "it will report returncode 255", self._pid)
        else:
            if os.WIFSIGNALED(status):
                self._returncode = -os.WTERMSIG(status)
            elif os.WIFEXITED(status):
                self._returncode = os.WEXITSTATUS(status)
            else:
                self._returncode = status
        finally:
            self._terminated = True
        return self._returncode


class PidfdChildProcess(AbstractChildProcess):
    def __init__(self, pid: int, pidfd: int) -> None:
        self._pid = pid
        self._pidfd = pidfd
        self._returncode = None
        self._wait_event = asyncio.Event()
        self._terminated = False
        loop = asyncio.get_running_loop()
        loop.add_reader(self._pidfd, self._do_wait)

    def send_signal(self, signum: int) -> None:
        if self._terminated:
            if signum != signal.SIGKILL:
                logger.warning(
                    "PidfdChildProcess(%d, %d).send_signal(%d): " "The process has already terminated.",
                    self._pid,
                    self._pidfd,
                    signum,
                )
            return
        if signum == signal.SIGKILL:
            logger.warning("Force-killed hanging child: %d", self._pid)
        signal.pidfd_send_signal(self._pidfd, signum)  # type: ignore

    def _do_wait(self):
        loop = asyncio.get_running_loop()
        try:
            status_info = os.waitid(os.P_PIDFD, self._pidfd, os.WEXITED | 0x40000000)  # type: ignore
        except ChildProcessError:
            self._returncode = 255
            logger.warning("child process %d exit status already read: " "it will report returncode 255", self._pid)
        else:
            if status_info.si_code == os.CLD_KILLED:  # type: ignore
                self._returncode = -status_info.si_status  # signal number
            elif status_info.si_code == os.CLD_EXITED:
                self._returncode = status_info.si_status
            elif status_info.si_code == os.CLD_DUMPED:
                self._returncode = -status_info.si_status  # signal number
            else:
                logger.warning(
                    "unexpected si_code %d and si_status %d for child process %d",
                    status_info.si_code,
                    status_info.si_status,
                    self._pid,
                )
                self._returncode = 255
        finally:
            loop.remove_reader(self._pidfd)
            os.close(self._pidfd)
            self._terminated = True
            self._wait_event.set()

    async def wait(self) -> int:
        await self._wait_event.wait()
        assert self._returncode is not None
        return self._returncode


def _child_main(init_func, init_pipe, child_func: Callable[[], int]) -> int:
    if init_func is not None:
        init_func()

    os.write(init_pipe, b"\0")
    os.close(init_pipe)
    return child_func()


async def _fork_posix(child_func: Callable[[], int]) -> int:
    loop = asyncio.get_running_loop()
    init_pipe = os.pipe()

    pid = os.fork()
    if pid == 0:
        os.close(init_pipe[0])
        ret = 0
        try:
            ret = _child_main(None, init_pipe[1], child_func)
        except KeyboardInterrupt:
            ret = -signal.SIGINT
        finally:
            os._exit(ret)
        return ret
    os.close(init_pipe[1])

    # Wait for the child's readiness notification
    init_event = asyncio.Event()
    loop.add_reader(init_pipe[0], init_event.set)
    await init_event.wait()
    loop.remove_reader(init_pipe[0])
    os.read(init_pipe[0], 1)
    os.close(init_pipe[0])
    return pid


async def _clone_pidfd(child_func: Callable[[], int]) -> Tuple[int, int]:
    loop = asyncio.get_running_loop()
    init_pipe = os.pipe()

    pid = os.fork()
    if pid == 0:
        os.close(init_pipe[0])
        ret = 0
        try:
            ret = _child_main(None, init_pipe[1], child_func)
        except KeyboardInterrupt:
            ret = -signal.SIGINT
        finally:
            os._exit(ret)
        return ret
    os.close(init_pipe[1])

    # Get the pidfd.
    fd = os.pidfd_open(pid, 0)  # type: ignore

    # Wait for the child's readiness notification
    init_event = asyncio.Event()
    loop.add_reader(init_pipe[0], init_event.set)
    await init_event.wait()
    loop.remove_reader(init_pipe[0])
    os.read(init_pipe[0], 1)
    os.close(init_pipe[0])
    return pid, fd


async def afork(child_func: Callable[[], int]) -> AbstractChildProcess:
    if _has_pidfd:
        pid, pidfd = await _clone_pidfd(child_func)
        return PidfdChildProcess(pid, pidfd)
    else:
        pid = await _fork_posix(child_func)
        return PosixChildProcess(pid)
