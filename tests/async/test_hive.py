from threading import Thread, Event as _Event
import asyncio
import time
import _thread

import pybeehive
import pybeehive.async
from pybeehive.async import async_generator
from pybeehive.async.hive import _loop_async


def run_kill_hive(hive, wait=0.005):
    async def kill():
        await asyncio.sleep(wait)
        hive.kill()
    loop = asyncio.get_event_loop()

    hive._set_loop()
    with hive._setup_teardown_listeners():
        with hive._setup_teardown_streamers() as jobs:
            futures = [asyncio.ensure_future(j) for j in jobs]
            loop.run_until_complete(asyncio.gather(
                kill(),
                _loop_async(
                    hive._event_queue, hive.listeners, hive.kill_event
                )
            ))
    for future in futures:
        if not future.done():
            future.cancel()


def test_run(async_hive, async_bee_factory):
    listener = async_bee_factory.create('listener')
    streamer = async_bee_factory.create('streamer')
    async_hive.add(listener)
    async_hive.add(streamer)
    run_kill_hive(async_hive)
    assert len(listener.calls) > 0, 'Run did not yield any events'
    assert isinstance(listener.calls[0], pybeehive.Event), 'Run did not wrap non event data'


def test_decorated_listener(async_hive):
    calls = []

    @async_hive.listener
    async def on_event(event):
        calls.append(event)
        await asyncio.sleep(0)

    assert len(async_hive.listeners) == 1, 'Did not register listener to hive'
    async_hive.submit_event(pybeehive.Event('test'))
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
    assert isinstance(listener.calls[0], pybeehive.Event), 'Streamer did not yield correct data'


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
    assert isinstance(listener.calls[0], pybeehive.Event), 'Streamer did not yield correct data'


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
    assert isinstance(listener.calls[0], pybeehive.Event), 'Streamer did not yield correct data'


def test_only_streamers(async_hive):
    q = asyncio.Queue()
    d = {'count': 0}

    @async_hive.streamer
    async def publish():
        await q.put(1)
        # need to yield some time to event loop
        # or the test never returns
        await asyncio.sleep(1e-5)

    @async_hive.streamer
    async def pull():
        try:
            q.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(1e-4)
        else:
            d['count'] += 1
            await asyncio.sleep(0)

    run_kill_hive(async_hive)
    async_hive.close()
    assert d['count'] > 0, 'Did not get any data from queue'


def test_threaded_run(async_hive, async_bee_factory):
    stop = _Event()
    listener = async_bee_factory.create('listener')
    streamer = async_bee_factory.create('streamer')
    old_on_event = listener.on_event

    async def on_event(event):
        if not stop.is_set():
            stop.set()
        else:
            async_hive.kill()
            await asyncio.sleep(0)
        return await old_on_event(event)

    listener.on_event = on_event
    async_hive.add(listener)
    async_hive.add(streamer)
    async_hive.run(threaded=True)
    stop.wait()
    assert len(listener.calls) > 0, 'Streamer did not yield any events'
    assert isinstance(listener.calls[0], pybeehive.Event), 'Streamer did not yield correct data'


def test_close_hive(async_hive):
    run = _Event()

    @async_hive.streamer
    @async_generator
    async def stream():
        await asyncio.sleep(1e-6)
        return 1

    @async_hive.listener
    async def on_event(event):
        run.set()
        await asyncio.sleep(0)

    def close_after():
        run.wait()
        async_hive.close()

    Thread(target=close_after).start()
    async_hive.run()
    assert run.is_set(), 'Did not run on_event before close'


def test_close_hive_only_streamers(async_hive):
    q = asyncio.Queue()
    d = {'count': 0}

    @async_hive.streamer
    @async_generator
    async def publish():
        await q.put(1)
        # need to yield some time to event loop
        # or the test never returns
        await asyncio.sleep(1e-5)

    @async_hive.streamer
    @async_generator
    async def pull():
        try:
            q.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(1e-5)
        else:
            d['count'] += 1
            await asyncio.sleep(0)

    def close_after():
        time.sleep(1e-3)
        async_hive.close()

    Thread(target=close_after).start()
    async_hive.run()
    assert d['count'] > 0, 'Did not get any data from queue'


def test_interrupted_run(async_hive):
    def interrupt():
        time.sleep(5e-3)
        _thread.interrupt_main()

    @async_hive.streamer
    @async_generator
    async def stream():
        await asyncio.sleep(1e-6)
        return 1

    Thread(target=interrupt).start()
    async_hive.run()
    assert not async_hive.alive, 'KeyboardInterrupt did not kill hive'


def test_setup_teardown_exceptions(async_hive, async_bee_factory):
    l_failed_setup = async_bee_factory.create('listener')
    l_failed_teardown = async_bee_factory.create('listener')
    l_failed_both = async_bee_factory.create('listener')
    s_failed_setup = async_bee_factory.create('streamer')
    s_failed_teardown = async_bee_factory.create('streamer')
    s_failed_both = async_bee_factory.create('streamer')

    async def fail(*args, **kwargs):
        raise ValueError

    for bee in [l_failed_setup, s_failed_setup, l_failed_both, s_failed_both]:
        bee.setup = fail
    for bee in [l_failed_teardown, s_failed_teardown, l_failed_both, s_failed_both]:
        bee.teardown = fail

    for bee in [
        l_failed_setup, s_failed_setup,
        l_failed_teardown, s_failed_teardown,
        l_failed_both, s_failed_both
    ]:
        async_hive.add(bee)

    run_kill_hive(async_hive)

    for bee in [l_failed_teardown, l_failed_setup, l_failed_both]:
        assert len(bee.calls) > 0, 'Listener with failed setup did not run'

    for bee in [s_failed_teardown, s_failed_setup, s_failed_both]:
        assert bee.count > 0, 'Streamer with failed setup did not run'

    assert l_failed_setup.teardown_event.is_set(), \
        'Did not attempt to teardown listener with failed setup'
    assert s_failed_setup.teardown_event.is_set(), \
        'Did not attempt to teardown streamer with failed setup'
