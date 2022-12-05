from __future__ import annotations

import inspect
import io
import os
import pathlib
import shutil
import tempfile
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from os import PathLike
from types import TracebackType
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    AnyStr,
    AsyncIterator,
    Callable,
    Final,
    Generic,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
    overload,
    runtime_checkable,
)

from . import to_thread
from cbasyncio.types import AsyncResource

if TYPE_CHECKING:
    from _typeshed import OpenBinaryMode, OpenTextMode, ReadableBuffer, WriteableBuffer
else:
    ReadableBuffer = OpenBinaryMode = OpenTextMode = WriteableBuffer = object


@runtime_checkable
class AsyncIO(Protocol):
    async def read(self, size: Optional[int] = -1) -> Optional[bytes]:
        ...

    async def readall(self) -> Optional[bytes]:
        ...

    async def readinto(self, target: WriteableBuffer) -> int:
        ...

    async def readinto1(self, target: WriteableBuffer) -> int:
        ...

    async def seek(self, offset: int, whence: Union[int, Literal[0, 1, 2]] = ...) -> int:
        ...

    async def truncate(self, pos: Optional[int] = ...) -> int:
        ...

    async def write(self, b: bytes) -> int:
        ...

    async def flush(self) -> None:
        ...

    async def tell(self) -> int:
        ...

    async def close(self) -> None:
        ...

    def readable(self) -> bool:
        ...

    def seekable(self) -> bool:
        ...

    def writable(self) -> bool:
        ...


class AsyncFile(AsyncResource, AsyncIO, Generic[AnyStr]):
    def __init__(self, fp: IO[AnyStr]) -> None:
        self._fp: Any = fp

    def __getattr__(self, name: str) -> object:
        return getattr(self._fp, name)

    def readable(self) -> bool:
        return self._fp.readable()

    def seekable(self) -> bool:
        return self._fp.seekable()

    def writable(self) -> bool:
        return self._fp.writable()

    @property
    def wrapped(self) -> IO[AnyStr]:
        return self._fp

    async def __aiter__(self) -> AsyncIterator[AnyStr]:
        while True:
            line = await self.readline()
            if line:
                yield line
            else:
                break

    async def aiter_bytes(self: AsyncFile[bytes], chunk_size: int = io.DEFAULT_BUFFER_SIZE) -> AsyncIterator[bytes]:
        ba = bytearray(chunk_size)
        while n := await self.readinto(ba):
            yield bytes(ba[:n])

    async def aclose(self) -> None:
        return await to_thread.run_sync(self._fp.close)

    async def close(self) -> None:
        return await self.aclose()

    async def readall(self) -> bytes:
        readall = getattr(self._fp, "readall")
        if not inspect.ismethod(readall):
            raise AttributeError("file object has no attribute 'readall'")
        return await to_thread.run_sync(readall)

    async def read(self, size: int = -1) -> AnyStr:
        return await to_thread.run_sync(self._fp.read, size)

    async def read1(self: AsyncFile[bytes], size: int = -1) -> bytes:
        return await to_thread.run_sync(self._fp.read1, size)

    async def readline(self) -> AnyStr:
        return await to_thread.run_sync(self._fp.readline)

    async def readlines(self) -> List[AnyStr]:
        return await to_thread.run_sync(self._fp.readlines)

    async def readinto(self: AsyncFile[bytes], b: WriteableBuffer) -> int:
        return await to_thread.run_sync(self._fp.readinto, b)

    async def readinto1(self: AsyncFile[bytes], b: WriteableBuffer) -> int:
        return await to_thread.run_sync(self._fp.readinto1, b)

    @overload
    async def write(self: AsyncFile[bytes], b: ReadableBuffer) -> int:
        ...

    @overload
    async def write(self: AsyncFile[str], b: str) -> int:
        ...

    async def write(self, b: Union[ReadableBuffer, str]) -> int:
        return await to_thread.run_sync(self._fp.write, b)

    @overload
    async def writelines(self: AsyncFile[bytes], lines: Iterable[ReadableBuffer]) -> None:
        ...

    @overload
    async def writelines(self: AsyncFile[str], lines: Iterable[str]) -> None:
        ...

    async def writelines(self, lines: Union[Iterable[ReadableBuffer], Iterable[str]]) -> None:
        return await to_thread.run_sync(self._fp.writelines, lines)

    async def truncate(self, size: Optional[int] = None) -> int:
        return await to_thread.run_sync(self._fp.truncate, size)

    async def seek(self, offset: int, whence: Optional[int] = os.SEEK_SET) -> int:
        return await to_thread.run_sync(self._fp.seek, offset, whence)

    async def tell(self) -> int:
        return await to_thread.run_sync(self._fp.tell)

    async def flush(self) -> None:
        return await to_thread.run_sync(self._fp.flush)


@overload
async def open_file(
    file: Union[str, "PathLike[str]", int],
    mode: OpenBinaryMode,
    buffering: int = ...,
    encoding: Optional[str] = ...,
    errors: Optional[str] = ...,
    newline: Optional[str] = ...,
    closefd: bool = ...,
    opener: Optional[Callable[[str, int], int]] = ...,
) -> AsyncFile[bytes]:
    ...


@overload
async def open_file(
    file: Union[str, "PathLike[str]", int],
    mode: OpenTextMode = ...,
    buffering: int = ...,
    encoding: Optional[str] = ...,
    errors: Optional[str] = ...,
    newline: Optional[str] = ...,
    closefd: bool = ...,
    opener: Optional[Callable[[str, int], int]] = ...,
) -> AsyncFile[str]:
    ...


async def open_file(
    file: Union[str, "PathLike[str]", int],
    mode: str = "r",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    closefd: bool = True,
    opener: Optional[Callable[[str, int], int]] = None,
) -> AsyncFile[Any]:
    fp = await to_thread.run_sync(open, file, mode, buffering, encoding, errors, newline, closefd, opener)
    return AsyncFile(fp)


def wrap_file(file: IO[AnyStr]) -> AsyncFile[AnyStr]:
    return AsyncFile(file)


@asynccontextmanager
async def temporary_file(
    suffix: Optional[str] = None,
    prefix: Optional[str] = None,
    dir: Optional[str] = None,
) -> AsyncIterator[Path]:
    fd, path = await to_thread.run_sync(tempfile.mkstemp, suffix, prefix, dir)
    await to_thread.run_sync(os.close, fd)
    temp_file_path = Path(path)
    yield temp_file_path
    await temp_file_path.unlink(missing_ok=True)


@asynccontextmanager
async def temporary_directory(
    suffix: Optional[str] = None,
    prefix: Optional[str] = None,
    dir: Optional[str] = None,
) -> AsyncIterator[Path]:
    tempdir = Path(await to_thread.run_sync(tempfile.mkdtemp, suffix, prefix, dir))
    yield tempdir
    await to_thread.run_sync(partial(shutil.rmtree, str(tempdir), ignore_errors=True))


def get_temp_filepath(prefix: str = "", suffix: str = "", dir: Union[str, Path, None] = None) -> Path:
    basedir = dir or tempfile.gettempdir()
    filename = prefix + uuid.uuid4().hex + suffix
    return Path(basedir) / filename


@dataclass(eq=False)
class _PathIterator(AsyncIterator["Path"]):
    iterator: Iterator["PathLike[str]"]

    async def __anext__(self) -> Path:
        nextval = await to_thread.run_sync(next, self.iterator, None, cancellable=True)
        if nextval is None:
            raise StopAsyncIteration from None

        return Path(cast(PathLike[str], nextval))


class Path:
    __slots__ = "_path", "__weakref__"
    __weakref__: Any

    def __init__(self, *args: Union[str, "PathLike[str]"]) -> None:
        self._path: Final[pathlib.Path] = pathlib.Path(*args)

    def __fspath__(self) -> str:
        return self._path.__fspath__()

    def __str__(self) -> str:
        return self._path.__str__()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.as_posix()!r})"

    def __bytes__(self) -> bytes:
        return self._path.__bytes__()

    def __hash__(self) -> int:
        return self._path.__hash__()

    def __eq__(self, other: object) -> bool:
        target = other._path if isinstance(other, Path) else other
        return self._path.__eq__(target)

    def __lt__(self, other: "Path") -> bool:
        target = other._path if isinstance(other, Path) else other
        return self._path.__lt__(target)

    def __le__(self, other: "Path") -> bool:
        target = other._path if isinstance(other, Path) else other
        return self._path.__le__(target)

    def __gt__(self, other: "Path") -> bool:
        target = other._path if isinstance(other, Path) else other
        return self._path.__gt__(target)

    def __ge__(self, other: "Path") -> bool:
        target = other._path if isinstance(other, Path) else other
        return self._path.__ge__(target)

    def __truediv__(self, other: Any) -> "Path":
        return Path(self._path / other)

    def __rtruediv__(self, other: Any) -> "Path":
        return Path(other) / self

    @property
    def parts(self) -> Tuple[str, ...]:
        return self._path.parts

    @property
    def drive(self) -> str:
        return self._path.drive

    @property
    def root(self) -> str:
        return self._path.root

    @property
    def anchor(self) -> str:
        return self._path.anchor

    @property
    def parents(self) -> Sequence["Path"]:
        return tuple(Path(p) for p in self._path.parents)

    @property
    def parent(self) -> "Path":
        return Path(self._path.parent)

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def suffix(self) -> str:
        return self._path.suffix

    @property
    def suffixes(self) -> List[str]:
        return self._path.suffixes

    @property
    def stem(self) -> str:
        return self._path.stem

    async def absolute(self) -> "Path":
        path = await to_thread.run_sync(self._path.absolute)
        return Path(path)

    def as_posix(self) -> str:
        return self._path.as_posix()

    def as_uri(self) -> str:
        return self._path.as_uri()

    def match(self, path_pattern: str) -> bool:
        return self._path.match(path_pattern)

    def is_relative_to(self, *other: Union[str, "PathLike[str]"]) -> bool:
        try:
            self.relative_to(*other)
            return True
        except ValueError:
            return False

    async def chmod(self, mode: int, *, follow_symlinks: bool = True) -> None:
        func = partial(os.chmod, follow_symlinks=follow_symlinks)
        return await to_thread.run_sync(func, self._path, mode)

    @classmethod
    async def cwd(cls) -> "Path":
        path = await to_thread.run_sync(pathlib.Path.cwd)
        return cls(path)

    async def exists(self) -> bool:
        return await to_thread.run_sync(self._path.exists, cancellable=True)

    async def expanduser(self) -> "Path":
        return Path(await to_thread.run_sync(self._path.expanduser, cancellable=True))

    def glob(self, pattern: str) -> AsyncIterator["Path"]:
        gen = self._path.glob(pattern)
        return _PathIterator(gen)

    async def group(self) -> str:
        return await to_thread.run_sync(self._path.group, cancellable=True)

    async def hardlink_to(self, target: Union[str, pathlib.Path, "Path"]) -> None:
        if isinstance(target, Path):
            target = target._path

        await to_thread.run_sync(os.link, target, self)

    @classmethod
    async def home(cls) -> "Path":
        home_path = await to_thread.run_sync(pathlib.Path.home)
        return cls(home_path)

    def is_absolute(self) -> bool:
        return self._path.is_absolute()

    async def is_block_device(self) -> bool:
        return await to_thread.run_sync(self._path.is_block_device, cancellable=True)

    async def is_char_device(self) -> bool:
        return await to_thread.run_sync(self._path.is_char_device, cancellable=True)

    async def is_dir(self) -> bool:
        return await to_thread.run_sync(self._path.is_dir, cancellable=True)

    async def is_fifo(self) -> bool:
        return await to_thread.run_sync(self._path.is_fifo, cancellable=True)

    async def is_file(self) -> bool:
        return await to_thread.run_sync(self._path.is_file, cancellable=True)

    async def is_mount(self) -> bool:
        return await to_thread.run_sync(os.path.ismount, self._path, cancellable=True)

    def is_reserved(self) -> bool:
        return self._path.is_reserved()

    async def is_socket(self) -> bool:
        return await to_thread.run_sync(self._path.is_socket, cancellable=True)

    async def is_symlink(self) -> bool:
        return await to_thread.run_sync(self._path.is_symlink, cancellable=True)

    def iterdir(self) -> AsyncIterator["Path"]:
        gen = self._path.iterdir()
        return _PathIterator(gen)

    def joinpath(self, *args: Union[str, "PathLike[str]"]) -> "Path":
        return Path(self._path.joinpath(*args))

    async def lchmod(self, mode: int) -> None:
        await to_thread.run_sync(self._path.lchmod, mode)

    async def lstat(self) -> os.stat_result:
        return await to_thread.run_sync(self._path.lstat, cancellable=True)

    async def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        await to_thread.run_sync(self._path.mkdir, mode, parents, exist_ok)

    @overload
    async def open(
        self,
        mode: OpenBinaryMode,
        buffering: int = ...,
        encoding: Optional[str] = ...,
        errors: Optional[str] = ...,
        newline: Optional[str] = ...,
    ) -> AsyncFile[bytes]:
        ...

    @overload
    async def open(
        self,
        mode: OpenTextMode = ...,
        buffering: int = ...,
        encoding: Optional[str] = ...,
        errors: Optional[str] = ...,
        newline: Optional[str] = ...,
    ) -> AsyncFile[str]:
        ...

    async def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> AsyncFile[Any]:
        fp = await to_thread.run_sync(self._path.open, mode, buffering, encoding, errors, newline)
        return AsyncFile(fp)

    async def owner(self) -> str:
        return await to_thread.run_sync(self._path.owner, cancellable=True)

    async def read_bytes(self) -> bytes:
        return await to_thread.run_sync(self._path.read_bytes)

    async def read_text(self, encoding: Optional[str] = None, errors: Optional[str] = None) -> str:
        return await to_thread.run_sync(self._path.read_text, encoding, errors)

    def relative_to(self, *other: Union[str, "PathLike[str]"]) -> "Path":
        return Path(self._path.relative_to(*other))

    async def readlink(self) -> "Path":
        target = await to_thread.run_sync(os.readlink, self._path)
        return Path(cast(str, target))

    async def rename(self, target: Union[str, pathlib.PurePath, "Path"]) -> "Path":
        if isinstance(target, Path):
            target = target._path

        await to_thread.run_sync(self._path.rename, target)
        return Path(target)

    async def replace(self, target: Union[str, pathlib.PurePath, "Path"]) -> "Path":
        if isinstance(target, Path):
            target = target._path

        await to_thread.run_sync(self._path.replace, target)
        return Path(target)

    async def resolve(self, strict: bool = False) -> "Path":
        func = partial(self._path.resolve, strict=strict)
        return Path(await to_thread.run_sync(func, cancellable=True))

    def rglob(self, pattern: str) -> AsyncIterator["Path"]:
        gen = self._path.rglob(pattern)
        return _PathIterator(gen)

    async def rmdir(self) -> None:
        await to_thread.run_sync(self._path.rmdir)

    async def samefile(self, other_path: Union[str, bytes, int, pathlib.Path, "Path"]) -> bool:
        if isinstance(other_path, Path):
            other_path = other_path._path

        return await to_thread.run_sync(self._path.samefile, other_path, cancellable=True)

    async def stat(self, *, follow_symlinks: bool = True) -> os.stat_result:
        func = partial(os.stat, follow_symlinks=follow_symlinks)
        return await to_thread.run_sync(func, self._path, cancellable=True)

    async def symlink_to(
        self,
        target: Union[str, pathlib.Path, "Path"],
        target_is_directory: bool = False,
    ) -> None:
        if isinstance(target, Path):
            target = target._path

        await to_thread.run_sync(self._path.symlink_to, target, target_is_directory)

    async def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        await to_thread.run_sync(self._path.touch, mode, exist_ok)

    async def unlink(self, missing_ok: bool = False) -> None:
        try:
            await to_thread.run_sync(self._path.unlink)
        except FileNotFoundError:
            if not missing_ok:
                raise

    def with_name(self, name: str) -> "Path":
        return Path(self._path.with_name(name))

    def with_stem(self, stem: str) -> "Path":
        return Path(self._path.with_name(stem + self._path.suffix))

    def with_suffix(self, suffix: str) -> "Path":
        return Path(self._path.with_suffix(suffix))

    async def write_bytes(self, data: bytes) -> int:
        return await to_thread.run_sync(self._path.write_bytes, data)

    async def write_text(
        self,
        data: str,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> int:
        # Path.write_text() does not support the "newline" parameter before Python 3.10
        def sync_write_text() -> int:
            with self._path.open("w", encoding=encoding, errors=errors, newline=newline) as fp:
                return fp.write(data)

        return await to_thread.run_sync(sync_write_text)


PathLike.register(Path)  # type: ignore


class TempFile:
    def __init__(
        self, suffix: Optional[str] = None, prefix: Optional[str] = None, dir: Union[Path, str, None] = None
    ) -> None:
        tempdir = Path(dir or tempfile.gettempdir())
        self._path = tempdir / ((prefix or "") + uuid.uuid4().hex + (suffix or ""))

    @property
    def path(self) -> Path:
        return self._path

    async def __aenter__(self) -> "TempFile":
        await self._path.touch(exist_ok=False)
        return self

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        await self._path.unlink(missing_ok=True)


class TempDir:
    _path: Path

    def __init__(self, suffix: Optional[str] = None, prefix: Optional[str] = None, dir: Optional[str] = None) -> None:
        self._path = Path((prefix or "") + (dir or tempfile.gettempdir()) + (suffix or ""))

    @property
    def path(self) -> Path:
        return self._path

    async def __aenter__(self) -> TempDir:
        await self._path.mkdir(parents=True, exist_ok=False)
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        await to_thread.run_sync(partial(shutil.rmtree, str(self._path), ignore_errors=True))

    def temp_file(self, prefix: Optional[str] = None, suffix: Optional[str] = None) -> Path:
        return self._path / ((prefix or "") + uuid.uuid4().hex + (suffix or ""))
