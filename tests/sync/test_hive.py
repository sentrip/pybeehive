import time
import pytest
import beehive

from multiprocessing import Event as _Event
from queue import Queue, Empty
from threading import Thread, Event


def run_kill_hive(hive, **kwargs):
    wait = kwargs.pop('wait', 0.03)

    def kill_after():
        time.sleep(wait)
        hive.kill()

    runner = Thread(target=kill_after)
    runner.start()
    hive.run(**kwargs)


def test_run(hive, bee_factory):
    listener = bee_factory.create('listener')
    streamer = bee_factory.create('streamer')
    hive.add(listener)
    hive.add(streamer)
    run_kill_hive(hive)
    assert len(listener.calls) > 0, 'Run did not yield any events'
    assert isinstance(listener.calls[0], beehive.Event), 'Run did not wrap non event data'


def test_decorated_listener(hive):
    calls = []

    @hive.listener
    def on_event(event):
        calls.append(event)
    assert len(hive.listeners) == 1, 'Did not register listener to hive'
    hive.submit_event(beehive.Event('test'))
    run_kill_hive(hive)
    assert len(calls) == 1, 'Listener did not execute on_event'
    assert calls[0].data == 'test', 'Listener did not return correct data'


def test_decorated_listener_chained(hive):
    chained_calls = []

    @hive.listener
    def listener1(event):
        return event

    @hive.listener(chain='listener1')
    def listener2(event):
        chained_calls.append(event)

    assert len(hive.listeners) == 1, 'Added chained listener to root list of listeners'
    assert len(hive.listeners._listeners['listener1'][0].chained_bees) == 1,\
        'Did not correctly chain listeners'
    hive.submit_event(beehive.Event('test'))
    run_kill_hive(hive)
    assert len(chained_calls) == 1, 'Chained listener did not execute on_event'


def test_decorated_listener_chained_multi(hive):
    chained_calls = []

    @hive.listener(filters=['topic1'])
    def listener1(event):
        return event

    @hive.listener(filters=['topic2'])
    def listener2(event):
        return event

    @hive.listener(chain=['listener1', 'listener2'])
    def listener3(event):
        chained_calls.append(event)

    assert len(hive.listeners) == 2, 'Added chained listener to root list of listeners'
    assert len(hive.listeners._listeners['listener1'][0].chained_bees) == 1,\
        'Did not correctly chain listeners'
    assert len(hive.listeners._listeners['listener2'][0].chained_bees) == 1,\
        'Did not correctly chain listeners'
    hive.submit_event(beehive.Event('test1', topic='topic1'))
    hive.submit_event(beehive.Event('test2', topic='topic2'))
    run_kill_hive(hive)
    assert len(chained_calls) == 2, 'Chained listener did not execute on_event from both chains'


def test_decorated_listener_with_filters(hive):
    all_calls = []
    filtered_calls = []

    @hive.listener
    def listener1(event):
        all_calls.append(event)

    @hive.listener(filters='topic1')
    def listener2(event):
        filtered_calls.append(event)

    hive.submit_event(beehive.Event('test'))
    hive.submit_event(beehive.Event('test1', topic='topic1'))
    run_kill_hive(hive)
    assert len(all_calls) == 2, 'Unfiltered listener did not execute all on_event calls'
    assert len(filtered_calls) == 1, 'Filtered listener did executed on_event when it shouldnt'


def test_decorated_listener_chained_with_filters(hive):
    topic1_calls = []
    topic2_calls = []

    @hive.listener
    def listener1(event):
        return event

    @hive.listener(chain=['listener1'], filters=['topic1'])
    def listener2(event):
        event.data = 'listener2'
        topic1_calls.append(event)

    @hive.listener(chain=['listener1'], filters=['topic2'])
    def listener3(event):
        event.data = 'listener3'
        topic2_calls.append(event)

    hive.submit_event(beehive.Event('test1', topic='topic1'))
    hive.submit_event(beehive.Event('test2', topic='topic2'))
    run_kill_hive(hive)
    assert len(topic1_calls) == 1, 'Chained filtered listener did not execute on_event'
    assert len(topic2_calls) == 1, 'Chained filtered listener did not execute on_event'
    assert topic1_calls[0].data == 'listener2', 'Chained filtered listener did not execute on_event'
    assert topic2_calls[0].data == 'listener3', 'Chained filtered listener did not execute on_event'


def test_decorated_streamer(hive, bee_factory):
    listener = bee_factory.create('listener')
    hive.add(listener)

    @hive.streamer
    def stream():
        for i in range(5):
            yield i
    assert len(hive.streamers) == 1, 'Did not register streamer to hive'
    run_kill_hive(hive)
    assert len(listener.calls) > 0, 'Streamer did not yield any events'
    assert isinstance(listener.calls[0], beehive.Event), 'Streamer did not yield correct data'


def test_decorated_streamer_with_topic(hive, bee_factory):
    listener = bee_factory.create('listener')
    hive.add(listener)

    @hive.streamer(topic='topic1')
    def stream():
        for i in range(5):
            yield i
    assert len(hive.streamers) == 1, 'Did not register streamer to hive'
    run_kill_hive(hive)
    assert len(listener.calls) > 0, 'Streamer did not yield any events'
    assert listener.calls[0].topic == 'topic1', 'Streamer did not yield correct data'


def test_only_streamers(hive):
    q = Queue()
    d = {'count': 0}

    @hive.streamer
    def publish():
        while True:
            q.put(1)
            yield

    @hive.streamer
    def pull():
        while True:
            try:
                q.get(timeout=0.001)
            except Empty:
                yield
            else:
                d['count'] += 1
                yield

    run_kill_hive(hive)
    hive.close()
    assert d['count'] > 0, 'Did not get any data from queue'


def test_threaded_run(hive, bee_factory):
    listener = bee_factory.create('listener')
    hive.add(listener)
    stop = _Event()

    @hive.streamer
    def stream():
        for i in range(5):
            yield i
        time.sleep(0.001)
        stop.set()
    hive.run(threaded=True)
    stop.wait()
    hive.kill()
    assert len(listener.calls) > 0, 'Streamer did not yield any events'
    assert isinstance(listener.calls[0], beehive.Event), 'Streamer did not yield correct data'


def test_close_hive(hive):
    run = Event()

    @hive.streamer
    def stream():
        while True:
            yield 1

    @hive.listener
    def on_event(event):
        run.set()

    def close_after():
        run.wait()
        hive.close()

    Thread(target=close_after).start()
    hive.run()
    assert run.is_set(), 'Did not run on_event before close'


def test_close_hive_only_streamers(hive):
    q = Queue()
    d = {'count': 0}

    @hive.streamer
    def publish():
        while True:
            time.sleep(1e-4)
            q.put(1)
            yield

    @hive.streamer
    def pull():
        while True:
            try:
                q.get(timeout=0.001)
            except Empty:
                yield
            else:
                d['count'] += 1
                yield

    def close_after():
        time.sleep(1e-3)
        hive.close()

    Thread(target=close_after).start()
    hive.run()
    assert d['count'] > 0, 'Did not get any data from queue'
