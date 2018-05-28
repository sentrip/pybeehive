from multiprocessing import Event as _Event
from queue import Queue, Empty
from threading import Thread, Event
import time
import _thread
import pytest
import pybeehive


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
    # Run once in debug mode to make sure,
    # but this should never cause errors
    run_kill_hive(hive, debug=True)
    assert len(listener.calls) > 0, 'Run did not yield any events'
    assert isinstance(listener.calls[0], pybeehive.Event), 'Run did not wrap non event data'


def test_decorated_listener(hive):
    calls = []

    @hive.listener
    def on_event(event):
        calls.append(event)
    assert len(hive.listeners) == 1, 'Did not register listener to hive'
    hive.submit_event(pybeehive.Event('test'))
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
    hive.submit_event(pybeehive.Event('test'))
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
    hive.submit_event(pybeehive.Event('test1', topic='topic1'))
    hive.submit_event(pybeehive.Event('test2', topic='topic2'))
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

    hive.submit_event(pybeehive.Event('test'))
    hive.submit_event(pybeehive.Event('test1', topic='topic1'))
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

    hive.submit_event(pybeehive.Event('test1', topic='topic1'))
    hive.submit_event(pybeehive.Event('test2', topic='topic2'))
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
    assert isinstance(listener.calls[0], pybeehive.Event), 'Streamer did not yield correct data'


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
    stop = _Event()
    listener = bee_factory.create('listener')
    streamer = bee_factory.create('streamer')
    old_on_event = listener.on_event

    def on_event(event):
        if not stop.is_set():
            stop.set()
        else:
            hive.kill()
        return old_on_event(event)

    listener.on_event = on_event
    hive.add(listener)
    hive.add(streamer)
    hive.run(threaded=True)
    stop.wait()
    assert len(listener.calls) > 0, 'Streamer did not yield any events'
    assert isinstance(listener.calls[0], pybeehive.Event), 'Streamer did not yield correct data'


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


def test_interrupted_run(hive):
    def interrupt():
        time.sleep(5e-3)
        _thread.interrupt_main()

    @hive.streamer
    def stream():
        while True:
            time.sleep(1e-6)
            return 1

    Thread(target=interrupt).start()
    hive.run()
    assert not hive.alive, 'KeyboardInterrupt did not kill hive'


def test_define_with_bad_args(hive):
    with pytest.raises(TypeError):
        hive.streamer(topic={'a': 1})(lambda: None)

    with pytest.raises(TypeError):
        hive.listener(chain=2)(lambda: None)

    with pytest.raises(TypeError):
        hive.listener(filters={'a': 1})(lambda: None)


def test_setup_teardown_exceptions(hive, bee_factory):
    l_failed_setup = bee_factory.create('listener')
    l_failed_teardown = bee_factory.create('listener')
    l_failed_both = bee_factory.create('listener')
    s_failed_setup = bee_factory.create('streamer')
    s_failed_teardown = bee_factory.create('streamer')
    s_failed_both = bee_factory.create('streamer')

    def fail(*args, **kwargs):
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
        hive.add(bee)

    run_kill_hive(hive)

    for bee in [l_failed_teardown, l_failed_setup, l_failed_both]:
        assert len(bee.calls) > 0, 'Listener with failed setup did not run'

    for bee in [s_failed_teardown, s_failed_setup, s_failed_both]:
        assert bee.count > 0, 'Streamer with failed setup did not run'

    assert l_failed_setup.teardown_event.is_set(), \
        'Did not attempt to teardown listener with failed setup'
    assert s_failed_setup.teardown_event.is_set(), \
        'Did not attempt to teardown streamer with failed setup'
