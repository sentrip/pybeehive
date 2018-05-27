import asyncio
import pybeehive
import pybeehive.async


def test_stream(async_bee_factory, run_in_loop):
    streamer = async_bee_factory.create('streamer')
    q = asyncio.Queue()
    streamer.set_queue(q)
    run_in_loop(streamer.run)
    assert not q.empty(), 'Stream did not yield any events'
    count = 0
    while not q.empty():
        event = q.get_nowait()
        assert isinstance(event, pybeehive.Event), 'Stream did not wrap non event data'
        assert event.data == count, 'Stream did not yield correct data'
        count += 1
    assert count == 10, 'Stream did not yield correct number of events'


def test_stream_pre_python36(async_bee_factory, run_in_loop):
    streamer = async_bee_factory.create('prepython36')
    results = []

    async def r():
        async for result in streamer.stream():
            results.append(result)

    run_in_loop(r)
    assert len(results) != 0, 'Generator stream did not yield any events'
    count = 0
    for i in results:
        count += 1
        assert i == count, 'Generator stream did not yield correct data'
    assert count == 12, 'Generator stream did not yield correct number of events'


def test_stream_with_exception(async_bee_factory, run_in_loop):
    streamer = async_bee_factory.create('streamer', failing=True)
    q = asyncio.Queue()
    streamer.set_queue(q)
    run_in_loop(streamer.run)
    assert not q.empty(), 'Stream did not yield any events'
    assert streamer.ex, 'Stream did not call on_exception'
    count = 0
    while not q.empty():
        event = q.get_nowait()
        assert isinstance(event, pybeehive.Event), 'Stream did not wrap non event data'
        assert event.data == count, 'Stream did not yield correct data'
        count += 1
    assert count == 3, 'Stream did not yield correct number of events'


def test_notify(async_bee_factory, run_in_loop):
    listener = async_bee_factory.create('listener')
    run_in_loop(listener.notify, 1)
    assert len(listener.calls) == 1, 'on_event did not trigger'
    assert listener.calls[0] == 1, 'Did not process data correct in on_event'


def test_chained_notify(async_bee_factory, run_in_loop):
    listener1 = async_bee_factory.create('listener')
    listener2 = async_bee_factory.create('listener')
    listener3 = async_bee_factory.create('listener')
    listener1.chain(listener2).chain(listener3)
    run_in_loop(listener1.notify, 1)
    assert len(listener2.calls) == 1, 'First chained on_event did not trigger'
    assert len(listener3.calls) == 1, 'Second chained on_event did not trigger'
    assert isinstance(listener2.calls[0], pybeehive.Event), 'Did convert data to event in chained on_event'
    assert isinstance(listener3.calls[0], pybeehive.Event), 'Did convert data to event in chained on_event'


def test_notify_with_exception(async_bee_factory, run_in_loop):
    listener = async_bee_factory.create('listener', failing=True)
    run_in_loop(listener.notify, 1)
    assert len(listener.calls) == 0, 'Failed on_event added data'


def test_chained_notify_with_exception(async_bee_factory, run_in_loop):
    listener1 = async_bee_factory.create('listener', failing=True)
    listener2 = async_bee_factory.create('listener', failing=True)
    listener3 = async_bee_factory.create('listener', failing=True)
    listener1.chain(listener2).chain(listener3)
    run_in_loop(listener1.notify, 1)
    assert len(listener2.calls) == 0, 'First chained on_event triggered'
    assert len(listener3.calls) == 0, 'Second chained on_event triggered'

