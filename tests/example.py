import asyncio
from typing import Any

from cbasyncio.aio import TaskGroup
from cbasyncio.streamz import SendChannel, Stream, create_channel_pair, stream
from cbasyncio.types import F


async def example_one():
    # Create a counting stream with a 0.2 seconds interval
    xs = stream.count(interval=0.2)

    # Operators can be piped using '|'
    ys = xs | stream.map.pipe(F[int](lambda x: x**2))

    # Streams can be sliced
    zs = ys[1:10:2]

    # Use a stream context for proper resource management
    async with zs.stream() as streamer:

        # Asynchronous iteration
        async for z in streamer:

            # Print 1, 9, 25, 49 and 81
            print("->", z)

    # Streams can be awaited and return the last value
    print("9² = ", await zs)

    # Streams can run several times
    print("9² = ", await zs)

    # Streams can be concatenated
    one_two_three = stream.just(1) + stream.range(2, 4)

    # Print [1, 2, 3]
    print(await stream.list(one_two_three))


async def example_two():
    # This stream computes 11² + 13² in 1.5 second
    xs = (
        stream.count(interval=0.1)  # Count from zero every 0.1 s
        | stream.skip.pipe(10)  # Skip the first 10 numbers
        | stream.take.pipe(5)  # Take the following 5
        | stream.filter.pipe(lambda x: x % 2)  # Keep odd numbers
        | stream.map.pipe(F[int](lambda x: x**2))  # Square the results
        | stream.accumulate.pipe()  # Add the numbers together
    )
    print("11² + 13² = ", await xs)


async def example_three():
    async def producer(chan: SendChannel[Any]):
        xs = stream.count(interval=0.3) | stream.take.pipe(30)
        async with chan, xs.stream() as streamer:
            async for x in streamer:
                await chan.send(x)

    async def consumer(id: int, s: Stream[int]):
        async with s.stream() as streamer:
            async for x in streamer:
                print(f"[{id}] -> {x}")

    sender, receiver = create_channel_pair(2)
    recv_stream = stream.from_channel(receiver) | stream.map.pipe(F[int](lambda x: x**2)) | stream.print.pipe("{}")

    # Start the producer
    # producer_task = asyncio.create_task(producer(sender))
    async with TaskGroup() as tg:
        tg.start_soon(producer, sender)
        for i in range(10):
            tg.start_soon(consumer, i, recv_stream)


# Run main coroutine
asyncio.run(example_three())
