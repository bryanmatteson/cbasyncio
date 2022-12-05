import asyncio
import contextlib
import warnings

import pytest

from cbasyncio.context import AsyncContextGroup, aclosing


@pytest.mark.asyncio
async def test_actxmgr(event_loop):

    step = 0

    @contextlib.asynccontextmanager
    async def simple_ctx(msg):
        nonlocal step
        step = 1
        await asyncio.sleep(0)
        step = 2
        try:
            yield msg
            step = 3
        finally:
            await asyncio.sleep(0)
            step = 4

    step = 0
    async with simple_ctx("hello") as msg:
        assert step == 2
        assert msg == "hello"
    assert step == 4

    step = 0
    try:
        async with simple_ctx("world") as msg:
            assert step == 2
            assert msg == "world"
            await asyncio.sleep(0)
            raise ValueError("something wrong")
    except Exception as e:
        await asyncio.sleep(0)
        assert e.args[0] == "something wrong"
        assert step == 4


@pytest.mark.asyncio
async def test_actxmgr_exception_in_context_body():

    # Exceptions raised in the context body
    # should be transparently raised.

    @contextlib.asynccontextmanager
    async def simple_ctx(msg):
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)

    with pytest.raises(ZeroDivisionError):
        async with simple_ctx("hello") as msg:
            assert msg == "hello"
            raise ZeroDivisionError

    exc = RuntimeError("oops")
    try:
        async with simple_ctx("hello") as msg:
            assert msg == "hello"
            raise exc
    except BaseException as e:
        assert e is exc
        assert e.args[0] == "oops"
    else:
        pytest.fail()

    cm = simple_ctx("hello")
    ret = await cm.__aenter__()
    assert ret == "hello"
    ret = await cm.__aexit__(ValueError, None, None)
    assert not ret


@pytest.mark.asyncio
async def test_actxmgr_exception_in_initialization():

    # Any exception before first yield is just transparently
    # raised out to the context block.

    @contextlib.asynccontextmanager
    async def simple_ctx1(msg):
        await asyncio.sleep(0)
        raise ZeroDivisionError
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)

    try:
        async with simple_ctx1("hello") as msg:
            assert msg == "hello"
    except ZeroDivisionError:
        pass
    else:
        pytest.fail()

    exc = RuntimeError("oops")

    @contextlib.asynccontextmanager
    async def simple_ctx(msg):
        await asyncio.sleep(0)
        raise exc
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)

    try:
        async with simple_ctx("hello") as msg:
            assert msg == "hello"
    except BaseException as e:
        assert e is exc
        assert e.args[0] == "oops"
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_exception_in_finalization():
    @contextlib.asynccontextmanager
    async def simple_ctx1(msg):
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)
        raise ZeroDivisionError
        await asyncio.sleep(0)

    try:
        async with simple_ctx1("hello") as msg:
            assert msg == "hello"
    except ZeroDivisionError:
        pass
    else:
        pytest.fail()

    exc = RuntimeError("oops")

    @contextlib.asynccontextmanager
    async def simple_ctx(msg):
        yield msg
        raise exc

    try:
        async with simple_ctx("hello") as msg:
            assert msg == "hello"
    except BaseException as e:
        assert e is exc
        assert e.args[0] == "oops"
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_exception_uncaught():
    @contextlib.asynccontextmanager
    async def simple_ctx(msg):
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)

    try:
        async with simple_ctx("hello"):
            raise IndexError("bomb")
    except BaseException as e:
        assert isinstance(e, IndexError)
        assert e.args[0] == "bomb"


@pytest.mark.asyncio
async def test_actxmgr_exception_nested():
    @contextlib.asynccontextmanager
    async def simple_ctx(msg):
        yield msg

    try:
        async with simple_ctx("hello") as msg1:
            async with simple_ctx("world") as msg2:
                assert msg1 == "hello"
                assert msg2 == "world"
                raise IndexError("bomb1")
    except BaseException as exc:
        assert isinstance(exc, IndexError)
        assert "bomb1" == exc.args[0]
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_exception_chained():
    @contextlib.asynccontextmanager
    async def simple_ctx(msg):
        try:
            await asyncio.sleep(0)
            yield msg
        except Exception as e:
            await asyncio.sleep(0)
            # exception is chained
            raise ValueError("bomb2") from e

    try:
        async with simple_ctx("hello") as msg:
            assert msg == "hello"
            raise IndexError("bomb1")
    except BaseException as exc:
        assert isinstance(exc, ValueError)
        assert "bomb2" == exc.args[0]
        assert isinstance(exc.__cause__, IndexError)
        assert "bomb1" == exc.__cause__.args[0]
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_exception_replaced():
    @contextlib.asynccontextmanager
    async def simple_ctx(msg):
        try:
            await asyncio.sleep(0)
            yield msg
        except Exception:
            await asyncio.sleep(0)
            # exception is replaced
            raise ValueError("bomb2")

    try:
        async with simple_ctx("hello") as msg:
            assert msg == "hello"
            raise IndexError("bomb1")
    except BaseException as exc:
        assert isinstance(exc, ValueError)
        assert "bomb2" == exc.args[0]
        assert exc.__cause__ is None
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_stopaiter(event_loop):
    @contextlib.asynccontextmanager
    async def simple_ctx1():
        await asyncio.sleep(0)
        yield 1
        await asyncio.sleep(0)
        yield 2
        await asyncio.sleep(0)

    cm = simple_ctx1()
    ret = await cm.__aenter__()
    assert ret == 1
    ret = await cm.__aexit__(StopAsyncIteration, None, None)
    assert ret is False

    step = 0

    @contextlib.asynccontextmanager
    async def simple_ctx():
        nonlocal step
        step = 1
        try:
            await asyncio.sleep(0)
            yield
        finally:
            step = 2
            await asyncio.sleep(0)
            raise StopAsyncIteration("x")

    try:
        step = 0
        async with simple_ctx():
            assert step == 1
    except RuntimeError:
        # converted to RuntimeError
        assert step == 2
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_transparency(event_loop):

    step = 0

    @contextlib.asynccontextmanager
    async def simple_ctx1():
        nonlocal step
        step = 1
        await asyncio.sleep(0)
        yield
        step = 2
        await asyncio.sleep(0)

    exc = StopAsyncIteration("x")
    try:
        step = 0
        async with simple_ctx1():
            assert step == 1
            raise exc
    except StopAsyncIteration as e:
        # exception is not handled
        assert e is exc
        assert step == 1
    else:
        pytest.fail()

    @contextlib.asynccontextmanager
    async def simple_ctx2():
        nonlocal step
        step = 1
        await asyncio.sleep(0)
        yield
        step = 2
        await asyncio.sleep(0)

    try:
        step = 0
        async with simple_ctx2():
            assert step == 1
            raise ValueError
    except ValueError:
        # exception is not handled
        assert step == 1
    else:
        pytest.fail()

    @contextlib.asynccontextmanager
    async def simple_ctx3():
        nonlocal step
        step = 1
        await asyncio.sleep(0)
        try:
            yield
        finally:
            step = 2
            await asyncio.sleep(0)

    try:
        exc = StopAsyncIteration("x")
        step = 0
        async with simple_ctx3():
            assert step == 1
            raise exc
    except StopAsyncIteration as e:
        # exception is not handled,
        # but context is finalized
        assert e is exc
        assert step == 2
    else:
        pytest.fail()

    @contextlib.asynccontextmanager
    async def simple_ctx4():
        nonlocal step
        step = 1
        await asyncio.sleep(0)
        try:
            yield
        finally:
            step = 2
            await asyncio.sleep(0)

    try:
        step = 0
        async with simple_ctx4():
            assert step == 1
            raise ValueError
    except ValueError:
        # exception is not handled,
        # but context is finalized
        assert step == 2
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_no_stop(event_loop):
    @contextlib.asynccontextmanager
    async def simple_ctx1(msg):
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)

    try:
        async with simple_ctx1("hello") as msg:
            assert msg == "hello"
    except RuntimeError as exc:
        assert "didn't stop" in exc.args[0]
    else:
        pytest.fail()

    try:
        async with simple_ctx1("hello") as msg:
            assert msg == "hello"
            raise ValueError("oops")
    except ValueError as exc:
        assert exc.args[0] == "oops"
    else:
        pytest.fail()

    @contextlib.asynccontextmanager
    async def simple_ctx(msg):
        try:
            await asyncio.sleep(0)
            yield msg
        finally:
            await asyncio.sleep(0)
            yield msg

    try:
        async with simple_ctx("hello") as msg:
            assert msg == "hello"
            raise ValueError("oops")
    except RuntimeError as exc:
        assert "didn't stop after" in exc.args[0]
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_no_yield(event_loop):
    @contextlib.asynccontextmanager  # type: ignore
    async def no_yield_ctx1(msg):
        pass

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        try:
            async with no_yield_ctx1("hello"):
                pass
        except RuntimeError as exc:
            assert "must be an async-gen" in exc.args[0]
        except AttributeError:  # in Python 3.7
            pass
        except TypeError as exc:  # in Python 3.10
            assert "not an async iterator" in exc.args[0]
        else:
            pytest.fail()


@pytest.mark.asyncio
async def test_actxgroup(event_loop):

    # Test basic function.
    exit_count = 0

    @contextlib.asynccontextmanager
    async def ctx(a):
        nonlocal exit_count
        await asyncio.sleep(0)
        yield a + 10
        await asyncio.sleep(0)
        exit_count += 1

    ctxgrp = AsyncContextGroup()

    for i in range(3):
        ctxgrp.add(ctx(i))

    async with ctxgrp as values:
        assert values[0] == 10
        assert values[1] == 11
        assert values[2] == 12
        assert len(ctxgrp._cm_yields) == 3

    assert exit_count == 3
    assert len(ctxgrp._cm_yields) == 0
    returns = ctxgrp.exit_states()
    assert not returns[0]
    assert not returns[1]
    assert not returns[2]

    # Test generator/iterator initialization
    exit_count = 0
    ctxgrp = AsyncContextGroup(ctx(i) for i in range(3))

    async with ctxgrp as values:
        assert values[0] == 10
        assert values[1] == 11
        assert values[2] == 12
        assert len(ctxgrp._cm_yields) == 3

    assert exit_count == 3
    assert len(ctxgrp._cm_yields) == 0
    returns = ctxgrp.exit_states()
    assert not returns[0]
    assert not returns[1]
    assert not returns[2]


@pytest.mark.asyncio
async def test_actxgroup_exception_from_cm(event_loop):
    @contextlib.asynccontextmanager
    async def ctx1(a):
        await asyncio.sleep(0)
        raise asyncio.CancelledError
        await asyncio.sleep(0)
        yield a

    @contextlib.asynccontextmanager
    async def ctx2(a):
        await asyncio.sleep(0)
        raise ZeroDivisionError
        await asyncio.sleep(0)
        yield a

    ctxgrp = AsyncContextGroup([ctx1(1), ctx2(2)])

    async with ctxgrp as values:
        assert isinstance(values[0], asyncio.CancelledError)
        assert isinstance(values[1], ZeroDivisionError)

    @contextlib.asynccontextmanager
    async def ctx3(a):
        yield a
        raise asyncio.CancelledError

    @contextlib.asynccontextmanager
    async def ctx4(a):
        yield a
        raise ZeroDivisionError

    ctxgrp = AsyncContextGroup([ctx3(1), ctx4(2)])

    async with ctxgrp as values:
        assert values[0] == 1
        assert values[1] == 2

    exits = ctxgrp.exit_states()
    assert isinstance(exits[0], asyncio.CancelledError)
    assert isinstance(exits[1], ZeroDivisionError)


@pytest.mark.asyncio
async def test_actxgroup_exception_from_body(event_loop):

    exit_count = 0

    @contextlib.asynccontextmanager
    async def ctx1(a):
        nonlocal exit_count
        await asyncio.sleep(0)
        yield a
        await asyncio.sleep(0)
        # yield raises the exception from the context body.
        # If not handled, finalization will not be executed.
        exit_count += 1

    ctxgrp = AsyncContextGroup([ctx1(1), ctx1(2)])

    try:
        async with ctxgrp as values:
            assert values[0] == 1
            assert values[1] == 2
            raise ZeroDivisionError
    except Exception as e:
        assert isinstance(e, ZeroDivisionError)

    exits = ctxgrp.exit_states()
    assert not exits[0]  # __aexit__ are called successfully
    assert not exits[1]
    assert exit_count == 0  # but they errored internally

    exit_count = 0

    @contextlib.asynccontextmanager
    async def ctx(a):
        nonlocal exit_count
        try:
            await asyncio.sleep(0)
            yield a
        finally:
            await asyncio.sleep(0)
            # Ensure finalization is executed.
            exit_count += 1

    ctxgrp = AsyncContextGroup([ctx(1), ctx(2)])

    try:
        async with ctxgrp as values:
            assert values[0] == 1
            assert values[1] == 2
            raise ZeroDivisionError
    except Exception as e:
        assert isinstance(e, ZeroDivisionError)

    exits = ctxgrp.exit_states()
    assert not exits[0]  # __aexit__ are called successfully
    assert not exits[1]
    assert exit_count == 2  # they also suceeeded


@pytest.mark.asyncio
async def test_aclosing(event_loop):

    finalized = False

    async def myiter():
        nonlocal finalized
        try:
            yield 1
            await asyncio.sleep(0.2)
            yield 10
            await asyncio.sleep(0.2)
            yield 100
        except asyncio.CancelledError:
            await asyncio.sleep(0.2)
            finalized = True
            raise

    mysum = 0
    detect_cancellation = False

    async def mytask():
        nonlocal mysum, detect_cancellation
        try:
            async with aclosing(myiter()) as agen:
                async for x in agen:
                    mysum += x
        except asyncio.CancelledError:
            detect_cancellation = True

    t = event_loop.create_task(mytask())
    await asyncio.sleep(0.3)
    t.cancel()
    await t

    assert mysum == 11
    assert finalized
    assert detect_cancellation
