from __future__ import annotations

import asyncio
import math
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from types import TracebackType
from typing import (
    Any,
    AsyncIterable,
    Deque,
    Generic,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    Union,
    overload,
)

from typing_extensions import Self

from .._types import R, T
from ..errors import BrokenResourceError, ClosedResourceError, EndOfStream, WouldBlock
from cbasyncio.types import AsyncResource


class ChannelStatistics(NamedTuple):
    current_buffer_used: int
    """number of items stored in the buffer"""

    max_buffer_size: float
    """maximum number of items that can be stored on this stream (or :data:`math.inf`)"""

    open_send_streams: int
    """number of unclosed clones of the send stream"""

    open_receive_streams: int
    """number of unclosed clones of the receive stream"""

    tasks_waiting_send: int
    """number of tasks blocked on :meth:`MemoryObjectSendStream.send`"""

    tasks_waiting_receive: int
    """number of tasks blocked on :meth:`MemoryObjectReceiveStream.receive`"""


@dataclass(eq=False)
class ChannelState(Generic[T]):
    max_buffer_size: float = field()
    buffer: Deque[T] = field(init=False, default_factory=deque)
    open_send_channels: int = field(init=False, default=0)
    open_receive_channels: int = field(init=False, default=0)
    waiting_receivers: "OrderedDict[asyncio.Event, List[T]]" = field(init=False, default_factory=OrderedDict)
    waiting_senders: "OrderedDict[asyncio.Event, T]" = field(init=False, default_factory=OrderedDict)

    def statistics(self) -> ChannelStatistics:
        return ChannelStatistics(
            len(self.buffer),
            self.max_buffer_size,
            self.open_send_channels,
            self.open_receive_channels,
            len(self.waiting_senders),
            len(self.waiting_receivers),
        )


@dataclass(eq=False)
class RecvChannel(AsyncIterable[T], AsyncResource, Generic[T]):
    _state: ChannelState[T]
    _closed: bool = field(init=False, default=False)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        try:
            return await self.receive()
        except EndOfStream:
            raise StopAsyncIteration

    def __post_init__(self) -> None:
        self._state.open_receive_channels += 1

    def receive_nowait(self) -> T:
        if self._closed:
            raise ClosedResourceError

        if self._state.waiting_senders:
            send_event, item = self._state.waiting_senders.popitem(last=False)
            self._state.buffer.append(item)
            send_event.set()

        if self._state.buffer:
            return self._state.buffer.popleft()
        elif not self._state.open_send_channels:
            raise EndOfStream

        raise WouldBlock

    async def receive(self) -> T:
        await asyncio.sleep(0)
        try:
            return self.receive_nowait()
        except WouldBlock:
            receive_event = asyncio.Event()
            container: List[T] = []
            self._state.waiting_receivers[receive_event] = container

            try:
                await receive_event.wait()
            except asyncio.CancelledError:
                if not container:
                    raise
            finally:
                self._state.waiting_receivers.pop(receive_event, None)

            if container:
                return container[0]
            else:
                raise EndOfStream

    def clone(self) -> RecvChannel[T]:
        if self._closed:
            raise ClosedResourceError

        return RecvChannel(_state=self._state)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._state.open_receive_channels -= 1
            if self._state.open_receive_channels == 0:
                send_events = list(self._state.waiting_senders.keys())
                for event in send_events:
                    event.set()

    async def aclose(self) -> None:
        self.close()

    def statistics(self) -> ChannelStatistics:
        return self._state.statistics()

    def __enter__(self) -> RecvChannel[T]:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()


@dataclass(eq=False)
class SendChannel(AsyncResource, Generic[T]):
    _state: ChannelState[T]
    _closed: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self._state.open_send_channels += 1

    def send_nowait(self, item: T) -> None:
        if self._closed:
            raise ClosedResourceError
        if not self._state.open_receive_channels:
            raise BrokenResourceError

        if self._state.waiting_receivers:
            receive_event, container = self._state.waiting_receivers.popitem(last=False)
            container.append(item)
            receive_event.set()
        elif len(self._state.buffer) < self._state.max_buffer_size:
            self._state.buffer.append(item)
        else:
            raise WouldBlock

    async def send(self, item: T) -> None:
        await asyncio.sleep(0)
        try:
            self.send_nowait(item)
        except WouldBlock:
            send_event = asyncio.Event()
            self._state.waiting_senders[send_event] = item
            try:
                await send_event.wait()
            except BaseException:
                self._state.waiting_senders.pop(send_event, None)
                raise

            if self._state.waiting_senders.pop(send_event, None):
                raise BrokenResourceError

    async def send_from(self: SendChannel[Any], source: Iterable[R] | AsyncIterable[R], close: bool = False) -> None:
        if self._closed:
            raise ClosedResourceError

        if isinstance(source, AsyncIterable):
            async for item in source:
                await self.send(item)
        else:
            for item in source:
                await self.send(item)

        if close:
            self.close()

    def clone(self) -> SendChannel[T]:
        if self._closed:
            raise ClosedResourceError

        return SendChannel(_state=self._state)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._state.open_send_channels -= 1
            if self._state.open_send_channels == 0:
                receive_events = list(self._state.waiting_receivers.keys())
                self._state.waiting_receivers.clear()
                for event in receive_events:
                    event.set()

    async def aclose(self) -> None:
        self.close()

    def statistics(self) -> ChannelStatistics:
        return self._state.statistics()

    def __enter__(self) -> SendChannel[T]:
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        self.close()


class Channel(Generic[T]):
    def __init__(self, max_buffer_size: Union[int, float] = 0) -> None:
        if max_buffer_size != math.inf and not isinstance(max_buffer_size, int):
            raise ValueError("max_buffer_size must be either an integer or math.inf")

        if max_buffer_size < 0:
            raise ValueError("max_buffer_size cannot be negative")

        self._closed = False
        self._state = ChannelState(max_buffer_size)
        self._send_chan = SendChannel(self._state)
        self._recv_chan = RecvChannel(self._state)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        self.close()

    def __aiter__(self) -> RecvChannel[T]:
        return self._recv_chan

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        await self.aclose()

    def as_pair(self) -> Tuple[SendChannel[T], RecvChannel[T]]:
        return self._send_chan, self._recv_chan

    def clone_recv(self) -> RecvChannel[T]:
        if self._closed:
            raise ClosedResourceError
        return self._recv_chan.clone()

    def clone_send(self) -> SendChannel[T]:
        if self._closed:
            raise ClosedResourceError
        return self._send_chan.clone()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._send_chan.close()
            self._recv_chan.close()

    async def aclose(self) -> None:
        self.close()

    async def receive(self) -> T:
        return await self._recv_chan.receive()

    async def send(self, item: T) -> None:
        await self._send_chan.send(item)

    async def send_from(self, source: Iterable[T] | AsyncIterable[T], close: bool = False) -> None:
        await self._send_chan.send_from(source, close)

    def statistics(self) -> ChannelStatistics:
        return self._state.statistics()


@overload
def create_channel_pair(max_buffer_size: float, item_type: Type[T]) -> Tuple[SendChannel[T], RecvChannel[T]]:
    ...


@overload
def create_channel_pair(
    max_buffer_size: float = 0,
) -> Tuple[SendChannel[Any], RecvChannel[Any]]:
    ...


def create_channel_pair(max_buffer_size: float = 0, item_type: Type[T] = Any) -> Tuple[SendChannel[T], RecvChannel[T]]:
    if max_buffer_size != math.inf and not isinstance(max_buffer_size, int):
        raise ValueError("max_buffer_size must be either an integer or math.inf")

    if max_buffer_size < 0:
        raise ValueError("max_buffer_size cannot be negative")

    state = ChannelState(max_buffer_size)
    return SendChannel(state), RecvChannel(state)
