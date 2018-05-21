import asyncio
import time
import pytest

import beehive
import beehive.async
from beehive.async import async_generator
from beehive.async.hive import _loop_async
from threading import Thread, Event as _Event


def run_kill_hive(hive):
    async def kill():
        await asyncio.sleep(0.005)
        hive.kill()
    loop = asyncio.get_event_loop()

    hive._set_loop()
    with hive._setup_teardown_listeners():
        with hive._setup_teardown_streamers() as jobs:
            futures = [asyncio.ensure_future(j) for j in jobs]
            loop.run_until_complete(asyncio.gather(
                _loop_async(
                    hive._event_queue, hive.listeners, hive.kill_event
                ),
                kill()
            ))
    loop.run_until_complete(asyncio.gather(*futures))


def test_run(async_hive, async_bee_factory):
    listener = async_bee_factory.create('listener')
    streamer = async_bee_factory.create('streamer')
    async_hive.add(listener)
    async_hive.add(streamer)
    run_kill_hive(async_hive)
    assert len(listener.calls) > 0, 'Run did not yield any events'
    assert isinstance(listener.calls[0], beehive.Event), 'Run did not wrap non event data'


def test_decorated_listener(async_hive):
    calls = []

    @async_hive.listener
    async def on_event(event):
        calls.append(event)
        await asyncio.sleep(0)

    assert len(async_hive.listeners) == 1, 'Did not register listener to hive'
    async_hive.submit_event(beehive.Event('test'))
    run_kill_hive(async_hive)
    assert len(calls) == 1, 'Listener did not execute on_event'
    assert calls[0].data == 'test', 'Listener did not return correct data'


def test_decorated_streamer(async_hive, async_bee_factory):
    listener = async_bee_factory.create('listener')
    async_hive.add(listener)
    i = 0

    @async_hive.streamer
    @async_generator
    async def stream():
        nonlocal i
        await asyncio.sleep(0)
        i += 1
        if i < 5:
            return i - 1
        else:
            raise StopAsyncIteration
    assert len(async_hive.streamers) == 1, 'Did not register streamer to hive'
    run_kill_hive(async_hive)
    assert len(listener.calls) > 0, 'Streamer did not yield any events'
    assert isinstance(listener.calls[0], beehive.Event), 'Streamer did not yield correct data'


def test_decorated_streamer_pre_python36(async_hive, async_bee_factory):
    listener = async_bee_factory.create('listener')
    async_hive.add(listener)
    i = 0

    @async_hive.streamer
    @async_generator
    async def stream():
        nonlocal i
        await asyncio.sleep(0)
        i += 1
        if i < 5:
            return i
        raise StopAsyncIteration
    assert len(async_hive.streamers) == 1, 'Did not register streamer to hive'
    run_kill_hive(async_hive)
    assert len(listener.calls) > 0, 'Streamer did not yield any events'
    assert isinstance(listener.calls[0], beehive.Event), 'Streamer did not yield correct data'


def test_decorated_streamer_pre_python36_closure(async_hive, async_bee_factory):
    listener = async_bee_factory.create('listener')
    async_hive.add(listener)

    @async_hive.streamer
    def stream():
        i = 0

        @async_generator
        async def wrapped():
            nonlocal i
            await asyncio.sleep(0)
            i += 1
            if i < 5:
                return i
            raise StopAsyncIteration
        return wrapped()
    assert len(async_hive.streamers) == 1, 'Did not register streamer to hive'
    run_kill_hive(async_hive)
    assert len(listener.calls) > 0, 'Streamer did not yield any events'
    assert isinstance(listener.calls[0], beehive.Event), 'Streamer did not yield correct data'


def test_threaded_run(async_hive, async_bee_factory):
    listener = async_bee_factory.create('listener')
    async_hive.add(listener)
    stop = _Event()
    i = 0

    @async_hive.streamer
    @async_generator
    async def stream():
        nonlocal i
        await asyncio.sleep(0)
        i += 1
        if i < 5:
            return i
        await asyncio.sleep(0.001)
        stop.set()
        raise StopAsyncIteration

    async_hive.run(threaded=True)
    stop.wait()
    async_hive.close()
    assert len(listener.calls) > 0, 'Streamer did not yield any events'
    assert isinstance(listener.calls[0], beehive.Event), 'Streamer did not yield correct data'
