import os
import pathlib
import platform
import socket
import stat
from typing import Tuple

import pytest
from _pytest.tmpdir import TempPathFactory

from cbasyncio import AsyncFile, Path, open_file, wrap_file
from cbasyncio.utils import StreamIterable


class TestAsyncFile:
    @pytest.fixture(scope="class")
    def testdata(cls) -> bytes:
        return b"".join(bytes([i] * 1000) for i in range(10))

    @pytest.fixture
    def testdatafile(self, tmp_path_factory: TempPathFactory, testdata: bytes) -> pathlib.Path:
        file = tmp_path_factory.mktemp("file").joinpath("testdata")
        file.write_bytes(testdata)
        return file

    @pytest.mark.asyncio
    async def test_open_close(self, testdatafile: pathlib.Path) -> None:
        f = await open_file(testdatafile)
        await f.aclose()

    @pytest.mark.asyncio
    async def test_read(self, testdatafile: pathlib.Path, testdata: bytes) -> None:
        data = None
        async with await open_file(testdatafile, "rb") as f:
            data = await f.read()

        assert f.closed
        assert data == testdata

    @pytest.mark.asyncio
    async def test_write(self, testdatafile: pathlib.Path, testdata: bytes) -> None:
        async with await open_file(testdatafile, "ab") as f:
            await f.write(b"f" * 1000)

        assert testdatafile.stat().st_size == len(testdata) + 1000

    @pytest.mark.asyncio
    async def test_async_iteration(self, tmp_path: pathlib.Path) -> None:
        lines = ["blah blah\n", "foo foo\n", "bar bar"]
        testpath = tmp_path.joinpath("testfile")
        testpath.write_text("".join(lines), "ascii")
        async with await open_file(str(testpath)) as f:
            lines_i = iter(lines)
            async for line in f:
                assert line == next(lines_i)

    @pytest.mark.asyncio
    async def test_wrap_file(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testdata"
        with path.open("w") as fp:
            wrapped = wrap_file(fp)
            await wrapped.write("dummydata")

        assert path.read_text() == "dummydata"


class TestPath:
    @pytest.fixture
    def populated_tmpdir(self, tmp_path: pathlib.Path) -> pathlib.Path:
        tmp_path.joinpath("testfile").touch()
        tmp_path.joinpath("testfile2").touch()
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        subdir.joinpath("dummyfile1.txt").touch()
        subdir.joinpath("dummyfile2.txt").touch()
        return tmp_path

    @pytest.mark.asyncio
    async def test_properties(self) -> None:
        """Ensure that all public properties and methods are available on the async Path class."""
        path = pathlib.Path("/test/path/another/part")
        stdlib_properties = {p for p in dir(path) if p.startswith("__") or not p.startswith("_")}
        stdlib_properties.discard("link_to")
        stdlib_properties.discard("__class_getitem__")
        stdlib_properties.discard("__enter__")
        stdlib_properties.discard("__exit__")

        async_path = Path(path)
        anyio_properties = {p for p in dir(async_path) if p.startswith("__") or not p.startswith("_")}

        missing = stdlib_properties - anyio_properties
        assert not missing

    def test_repr(self) -> None:
        assert repr(Path("/foo")) == "Path('/foo')"

    def test_bytes(self) -> None:
        assert bytes(Path("/foo-åäö")) == os.fsencode(f"{os.path.sep}foo-åäö")

    def test_hash(self) -> None:
        assert hash(Path("/foo")) == hash(pathlib.Path("/foo"))

    def test_comparison(self) -> None:
        path1 = Path("/foo1")
        path2 = Path("/foo2")
        assert path1 < path2
        assert path1 <= path2
        assert path2 > path1
        assert path2 >= path1

    def test_truediv(self) -> None:
        result = Path("/foo") / "bar"
        assert isinstance(result, Path)
        assert result == pathlib.Path("/foo/bar")

    def test_rtruediv(self) -> None:
        result = "/foo" / Path("bar")
        assert isinstance(result, Path)
        assert result == pathlib.Path("/foo/bar")

    def test_parts_property(self) -> None:
        assert Path("/abc/xyz/foo.txt").parts == (os.path.sep, "abc", "xyz", "foo.txt")

    def test_root_property(self) -> None:
        assert Path("/abc/xyz/foo.txt").root == os.path.sep

    def test_anchor_property(self) -> None:
        assert Path("/abc/xyz/foo.txt.zip").anchor == os.path.sep

    def test_parents_property(self) -> None:
        parents = Path("/abc/xyz/foo.txt").parents
        assert len(parents) == 3
        assert all(isinstance(parent, Path) for parent in parents)
        assert str(parents[0]) == f"{os.path.sep}abc{os.path.sep}xyz"
        assert str(parents[1]) == f"{os.path.sep}abc"
        assert str(parents[2]) == os.path.sep

    def test_parent_property(self) -> None:
        parent = Path("/abc/xyz/foo.txt").parent
        assert isinstance(parent, Path)
        assert str(parent) == f"{os.path.sep}abc{os.path.sep}xyz"

    def test_name_property(self) -> None:
        assert Path("/abc/xyz/foo.txt.zip").name == "foo.txt.zip"

    def test_suffix_property(self) -> None:
        assert Path("/abc/xyz/foo.txt.zip").suffix == ".zip"

    def test_suffixes_property(self) -> None:
        assert Path("/abc/xyz/foo.tar.gz").suffixes == [".tar", ".gz"]

    def test_stem_property(self) -> None:
        assert Path("/abc/xyz/foo.txt.zip").stem == "foo.txt"

    @pytest.mark.asyncio
    async def test_absolute(self) -> None:
        result = await Path("../foo/bar").absolute()
        assert isinstance(result, Path)
        assert result == pathlib.Path.cwd() / "../foo/bar"

    def test_as_uri(self) -> None:
        if platform.system() == "Windows":
            assert Path("c:\\foo\\bar").as_uri() == "file:///c:/foo/bar"
        else:
            assert Path("/foo/bar").as_uri() == "file:///foo/bar"

    @pytest.mark.asyncio
    async def test_cwd(self) -> None:
        result = await Path.cwd()
        assert isinstance(result, Path)
        assert result == pathlib.Path.cwd()

    @pytest.mark.asyncio
    async def test_exists(self, tmp_path: pathlib.Path) -> None:
        assert not await Path("~/btelkbee").exists()
        assert await Path(tmp_path).exists()

    @pytest.mark.asyncio
    async def test_expanduser(self) -> None:
        result = await Path("~/btelkbee").expanduser()
        assert isinstance(result, Path)
        assert str(result) == os.path.expanduser(f"~{os.path.sep}btelkbee")

    @pytest.mark.asyncio
    async def test_home(self) -> None:
        result = await Path.home()
        assert isinstance(result, Path)
        assert result == pathlib.Path.home()

    @pytest.mark.parametrize(
        "arg, result",
        [
            ("c:/xyz" if platform.system() == "Windows" else "/xyz", True),
            ("../xyz", False),
        ],
    )
    def test_is_absolute(self, arg: str, result: bool) -> None:
        assert Path(arg).is_absolute() == result

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="Block devices are not available on Windows",
    )
    @pytest.mark.asyncio
    async def test_is_block_device(self) -> None:
        assert not await Path("/btelkbee").is_block_device()
        with os.scandir("/dev") as iterator:
            for entry in iterator:
                if stat.S_ISBLK(entry.stat().st_mode):
                    assert await Path(entry.path).is_block_device()
                    break
            else:
                pytest.skip("Could not find a suitable block device")

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="Character devices are not available on Windows",
    )
    @pytest.mark.asyncio
    async def test_is_char_device(self) -> None:
        assert not await Path("/btelkbee").is_char_device()
        assert await Path("/dev/random").is_char_device()

    @pytest.mark.asyncio
    async def test_is_dir(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "somedir"
        assert not await Path(path).is_dir()
        path.mkdir()
        assert await Path(path).is_dir()

    @pytest.mark.skipif(platform.system() == "Windows", reason="mkfifo() is not available on Windows")
    @pytest.mark.asyncio
    async def test_is_fifo(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "somefifo"
        assert not await Path(path).is_fifo()
        os.mkfifo(path)
        assert await Path(path).is_fifo()

    @pytest.mark.asyncio
    async def test_is_file(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "somefile"
        assert not await Path(path).is_file()
        path.touch()
        assert await Path(path).is_file()

    @pytest.mark.asyncio
    async def test_is_mount(self) -> None:
        assert not await Path("/gfobj4ewiotj").is_mount()
        assert await Path("/").is_mount()

    def test_is_reserved(self) -> None:
        expected_result = platform.system() == "Windows"
        assert Path("nul").is_reserved() == expected_result

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="UNIX sockets are not available on Windows",
    )
    @pytest.mark.asyncio
    async def test_is_socket(self, tmp_path_factory: TempPathFactory) -> None:
        path = tmp_path_factory.mktemp("unix").joinpath("socket")
        assert not await Path(path).is_socket()
        with socket.socket(socket.AF_UNIX) as sock:
            sock.bind(str(path))
            assert await Path(path).is_socket()

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="symbolic links are not supported on Windows",
    )
    @pytest.mark.asyncio
    async def test_is_symlink(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testfile"
        assert not await Path(path).is_symlink()
        path.symlink_to("/foo")
        assert await Path(path).is_symlink()

    @pytest.mark.parametrize("args, result", [(("/xyz", "abc"), True), (("/xyz", "baz"), False)])
    def test_is_relative_to(self, args: Tuple[str], result: bool) -> None:
        assert Path("/xyz/abc/foo").is_relative_to(*args) == result

    @pytest.mark.asyncio
    async def test_glob(self, populated_tmpdir: pathlib.Path) -> None:
        all_paths = []
        async for path in Path(populated_tmpdir).glob("**/*.txt"):
            assert isinstance(path, Path)
            all_paths.append(path.name)

        all_paths.sort()
        assert all_paths == ["dummyfile1.txt", "dummyfile2.txt"]

    @pytest.mark.asyncio
    async def test_rglob(self, populated_tmpdir: pathlib.Path) -> None:
        all_paths = []
        async for path in Path(populated_tmpdir).rglob("*.txt"):
            assert isinstance(path, Path)
            all_paths.append(path.name)

        all_paths.sort()
        assert all_paths == ["dummyfile1.txt", "dummyfile2.txt"]

    @pytest.mark.asyncio
    async def test_iterdir(self, populated_tmpdir: pathlib.Path) -> None:
        all_paths = []
        async for path in Path(populated_tmpdir).iterdir():
            assert isinstance(path, Path)
            all_paths.append(path.name)

        all_paths.sort()
        assert all_paths == ["subdir", "testfile", "testfile2"]

    def test_joinpath(self) -> None:
        path = Path("/foo").joinpath("bar")
        assert path == Path("/foo/bar")

    def test_match(self) -> None:
        assert Path("/foo/bar").match("/foo/*")
        assert not Path("/foo/bar").match("/baz/*")

    @pytest.mark.skipif(platform.system() == "Windows", reason="chmod() is not available on Windows")
    @pytest.mark.asyncio
    async def test_chmod(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testfile"
        path.touch(0o666)
        await Path(path).chmod(0o444)
        assert path.stat().st_mode & 0o777 == 0o444

    @pytest.mark.skipif(platform.system() == "Windows", reason="hard links are not supported on Windows")
    @pytest.mark.asyncio
    async def test_hardlink_to(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testfile"
        target = tmp_path / "link"
        target.touch()
        await Path(path).hardlink_to(Path(target))
        assert path.stat().st_nlink == 2
        assert target.stat().st_nlink == 2

    @pytest.mark.skipif(not hasattr(os, "lchmod"), reason="os.lchmod() is not available")
    @pytest.mark.asyncio
    async def test_lchmod(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testfile"
        path.symlink_to("/foo/bar/baz")
        await Path(path).lchmod(0o600)
        assert path.lstat().st_mode & 0o777 == 0o600

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="symbolic links are not supported on Windows",
    )
    @pytest.mark.asyncio
    async def test_lstat(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path.joinpath("testfile")
        path.symlink_to("/foo/bar/baz")
        result = await Path(path).lstat()
        assert isinstance(result, os.stat_result)

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="owner and group are not supported on Windows",
    )
    @pytest.mark.asyncio
    async def test_group(self, tmp_path: pathlib.Path) -> None:
        import grp

        group_name = grp.getgrgid(os.getegid()).gr_name
        assert await Path(tmp_path).group() == group_name

    @pytest.mark.asyncio
    async def test_mkdir(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testdir"
        await Path(path).mkdir()
        assert path.is_dir()

    @pytest.mark.asyncio
    async def test_open(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testfile"
        path.write_bytes(b"bibbitibobbitiboo")
        fp = await Path(path).open("rb")
        assert isinstance(fp, AsyncFile)
        assert fp.name == str(path)
        await fp.aclose()

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="owner and group are not supported on Windows",
    )
    @pytest.mark.asyncio
    async def test_owner(self, tmp_path: pathlib.Path) -> None:
        import pwd

        user_name = pwd.getpwuid(os.geteuid()).pw_name
        assert await Path(tmp_path).owner() == user_name

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="symbolic links are not supported on Windows",
    )
    @pytest.mark.asyncio
    async def test_readlink(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path.joinpath("testfile")
        path.symlink_to("/foo/bar/baz")
        link_target = await Path(path).readlink()
        assert isinstance(link_target, Path)
        assert str(link_target) == "/foo/bar/baz"

    @pytest.mark.asyncio
    async def test_read_bytes(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testfile"
        path.write_bytes(b"bibbitibobbitiboo")
        assert await Path(path).read_bytes() == b"bibbitibobbitiboo"

    @pytest.mark.asyncio
    async def test_read_text(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testfile"
        path.write_text("some text åäö", encoding="utf-8")
        assert await Path(path).read_text(encoding="utf-8") == "some text åäö"

    @pytest.mark.asyncio
    async def test_rename(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "somefile"
        path.touch()
        target = tmp_path / "anotherfile"
        result = await Path(path).rename(Path(target))
        assert isinstance(result, Path)
        assert result == target

    @pytest.mark.asyncio
    async def test_replace(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "somefile"
        path.write_text("hello")
        target = tmp_path / "anotherfile"
        target.write_text("world")
        result = await Path(path).replace(Path(target))
        assert isinstance(result, Path)
        assert result == target
        assert target.read_text() == "hello"

    @pytest.mark.asyncio
    async def test_resolve(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "somedir" / ".." / "somefile"
        result = await Path(path).resolve()
        assert result == tmp_path / "somefile"

    @pytest.mark.asyncio
    async def test_rmdir(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "somedir"
        path.mkdir()
        await Path(path).rmdir()
        assert not path.exists()

    @pytest.mark.asyncio
    async def test_samefile(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "somefile"
        path.touch()
        assert await Path(tmp_path / "somefile").samefile(Path(path))

    @pytest.mark.asyncio
    async def test_stat(self, tmp_path: pathlib.Path) -> None:
        result = await Path(tmp_path).stat()
        assert isinstance(result, os.stat_result)

    @pytest.mark.asyncio
    async def test_touch(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testfile"
        await Path(path).touch()
        assert path.is_file()

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="symbolic links are not supported on Windows",
    )
    @pytest.mark.asyncio
    async def test_symlink_to(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testfile"
        target = tmp_path / "link"
        await Path(path).symlink_to(Path(target))
        assert path.is_symlink()

    @pytest.mark.asyncio
    async def test_unlink(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testfile"
        path.touch()
        await Path(path).unlink()
        assert not path.exists()

    @pytest.mark.asyncio
    async def test_unlink_missing_file(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testfile"
        await Path(path).unlink(missing_ok=True)
        with pytest.raises(FileNotFoundError):
            await Path(path).unlink(missing_ok=False)

    def test_with_name(self) -> None:
        assert Path("/xyz/foo.txt").with_name("bar").name == "bar"

    def test_with_stem(self) -> None:
        assert Path("/xyz/foo.txt").with_stem("bar").name == "bar.txt"

    def test_with_suffix(self) -> None:
        assert Path("/xyz/foo.txt.gz").with_suffix(".zip").name == "foo.txt.zip"

    @pytest.mark.asyncio
    async def test_write_bytes(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testfile"
        await Path(path).write_bytes(b"bibbitibobbitiboo")
        assert path.read_bytes() == b"bibbitibobbitiboo"

    @pytest.mark.asyncio
    async def test_write_text(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "testfile"
        await Path(path).write_text("some text åäö", encoding="utf-8")
        assert path.read_text(encoding="utf-8") == "some text åäö"


class TestStreamIterable:
    _data = [bytearray(256) for _ in range(4)]

    def test_readinto(self):
        stream = StreamIterable(self._data)
        assert len(stream._current) == 0
        assert stream.readable()

        bytes_read = 0

        buffer = bytearray(256)
        n = stream.readinto(buffer)
        assert n == 256
        assert stream.tell() == 256
        assert len(stream._current) == 0
        bytes_read += n

        n = stream.readinto(buffer[:128])
        assert n == 128
        assert stream.tell() == bytes_read + 128
        assert len(stream._current) == 128
        bytes_read += n

        n = stream.readinto(buffer[129:])
        assert n == 127
        assert stream.tell() == bytes_read + 127
        assert len(stream._current) == 1
        bytes_read += n

    def test_zero_length_readinto(self):
        stream = StreamIterable(self._data)
        buffer = bytearray(0)
        n = stream.readinto(buffer)
        assert n == 0
        assert len(stream._current) == 0
        assert stream.tell() == 0

    def test_read(self):
        stream = StreamIterable(self._data)
        assert len(stream._current) == 0

        data = stream.read(256)
        assert data is not None
        assert len(data) == 256
        assert stream.tell() == 256
        assert len(stream._current) == 0


class TestAsyncReadStream:
    _data = [bytearray(256) for _ in range(4)]

    @pytest.mark.asyncio
    async def test_readinto_with_sync_stream(self):
        data = StreamIterable(self._data)
        stream = AsyncFile(data)
        assert stream.readable()

        bytes_read = 0

        buffer = bytearray(256)
        n = await stream.readinto(buffer)
        assert n == 256
        assert await stream.tell() == 256
        bytes_read += n

        n = await stream.readinto(buffer[:128])
        assert n == 128
        assert await stream.tell() == bytes_read + 128
        bytes_read += n

        n = await stream.readinto(buffer[129:])
        assert n == 127
        assert await stream.tell() == bytes_read + 127
        bytes_read += n

    @pytest.mark.asyncio
    async def test_zero_length_readinto(self):
        stream = AsyncFile(StreamIterable(self._data))
        buffer = bytearray(0)
        n = await stream.readinto(buffer)
        assert n == 0
        assert await stream.tell() == 0

    @pytest.mark.asyncio
    async def test_read(self):
        stream = AsyncFile(StreamIterable(self._data))

        data = await stream.read(256)
        assert data is not None
        assert len(data) == 256
        assert await stream.tell() == 256
