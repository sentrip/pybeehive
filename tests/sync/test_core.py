import time
import pybeehive
from multiprocessing import Queue
import pytest

_now = time.time()
test_event_data = [
        ("test", _now, None),
        ("test", _now, 'topic'),
        ("test\x15", _now, None),
        ("test", _now, 'topic|asraasffsf,\\78*mfjdfb'),
        ("test\r\n\tsdfnskduf*34023849\n\r\\'d", _now, 'abcd'),
        ("", _now, 'abcd'),
        ("abcd", _now, ''),
        ("", _now, None),
        ("a"*100000, _now, None),
        ("test", None, None),
        ("test", None, 'topic'),
        ("test\x15", None, None),
        ("test", None, 'topic|asraasffsf,\\78*mfjdfb'),
        ("test\r\n\tsdfnskduf*34023849\n\r\\'d", None, 'abcd'),
        ("", None, 'abcd'),
        ("abcd", None, ''),
        ("", None, None),
        ("a" * 100000, None, None)
    ]


@pytest.mark.parametrize('data,topic,created_at', test_event_data)
def test_event_string_conversion(data, topic, created_at):
    # for data, created_at, topic in test_event_data:
    event = pybeehive.Event(data, topic=topic, created_at=created_at)
    event_string = event.tostring()
    c_event = pybeehive.Event.fromstring(event_string)
    assert event == c_event, "String converted event not equal to original"


def test_create_event_id():
    now = time.time()
    id_1 = pybeehive.Event.create_id('test', now)
    id_2 = pybeehive.Event.create_id('test', now)
    id_3 = pybeehive.Event.create_id('test', now + 1)
    assert id_1 == id_2, 'Different ids for identical data'
    assert id_1 != id_3 != id_2, 'Same ids for different data'


def test_create_event():
    now = time.time()
    same_1 = pybeehive.Event('test', created_at=now)
    same_2 = pybeehive.Event(same_1)
    assert same_1 == same_2, \
        "Event did not create correctly from another Event"
    diff_1 = pybeehive.Event('test', topic='1', created_at=now)
    diff_2 = pybeehive.Event('test', topic='2', created_at=now)
    diff_3 = pybeehive.Event('test', topic='1', created_at=now + 1)
    diff_4 = pybeehive.Event('test1', topic='1', created_at=now)
    assert diff_1 != diff_2, "Events with different topics equal"
    assert diff_2 != diff_3, "Events with different created_at equal"
    assert diff_3 != diff_4, "Events with different data equal"
    diff_5 = pybeehive.Event(diff_1, topic='2')
    diff_6 = pybeehive.Event(diff_1, created_at=now + 1)
    assert diff_1 != diff_5, "Event created from event with new topic equal"
    assert diff_1 != diff_6, "Event created from event with new time equal"
    assert diff_5.topic == '2', 'Did not override topic correctly'
    assert diff_6.created_at == now + 1, 'Did not override time correctly'


def test_event_dunder_methods():
    now = time.time()
    event_1_a = pybeehive.Event('test_1', created_at=now)
    event_1_b = pybeehive.Event('test_1', created_at=now)
    event_1_c = pybeehive.Event('test_1', created_at=now, topic='thing')
    event_2 = pybeehive.Event('test_1', created_at=now + 1)
    event_3 = pybeehive.Event('test_2', created_at=now)
    assert event_1_a == event_1_b, "Identical events not equal"
    assert event_1_a != event_1_c, "Events with different topics equal"
    assert event_1_a != event_2, "Events with different times equal"
    assert event_1_a != event_3, "Events with different data equal"
    assert str(event_1_a) == str(event_1_b), "Event strings different"
    events = set()
    events.add(event_1_a)
    assert len(events) == 1, "Did not add event to set"
    events.add(event_1_b)
    assert len(events) == 1, "Added identical events to set twice"
    events.add(event_2)
    assert len(events) == 2, "Did not add different event to set"


def test_stream(bee_factory):
    q = Queue()
    streamer = bee_factory.create('streamer')
    streamer.set_queue(q)
    streamer.run()
    time.sleep(0.01)  # because somehow the above line is a race condition ?!
    assert not q.empty(), 'Stream did not yield any events'
    count = 0
    while not q.empty():
        event = q.get()
        assert isinstance(event, pybeehive.Event), 'Did not wrap non event data'
        assert event.data == count, 'Stream did not yield correct data'
        count += 1
    assert count == 10, 'Stream did not yield correct number of events'


def test_stream_with_exception(bee_factory):
    q = Queue()
    streamer = bee_factory.create('streamer', failing=True)
    streamer.set_queue(q)
    streamer.run()
    time.sleep(0.01)  # because somehow the above line is a race condition ?!
    assert not q.empty(), 'Stream did not yield any events'
    assert streamer.ex, 'Stream did not call on_exception'
    count = 0
    while not q.empty():
        event = q.get()
        assert isinstance(event, pybeehive.Event), 'Did not wrap non event data'
        assert event.data == count, 'Stream did not yield correct data'
        count += 1
    assert count == 3, 'Stream did not yield correct number of events'


def test_filter(bee_factory):
    non_filtered = bee_factory.create('listener')
    filtered = bee_factory.create('listener', filters=['topic1'])
    no_topic = pybeehive.Event('data')
    non_filtered.notify(no_topic)
    assert len(non_filtered.calls) == 1, "Listener with no filters did not call on_event"
    filtered.notify(no_topic)
    assert len(filtered.calls) == 0, "Listener with filters called on_event for an event with no topic"
    topic = pybeehive.Event('data', topic='topic1')
    non_filtered.notify(topic)
    assert len(non_filtered.calls) == 2, "Listener with no filters did not call on_event"
    filtered.notify(topic)
    assert len(filtered.calls) == 1, "Listener with filters did not call on_event for an event with topic"


def test_notify(bee_factory):
    listener = bee_factory.create('listener')
    listener.notify(1)
    assert len(listener.calls) == 1, 'on_event did not trigger'
    assert listener.calls[0] == 1, 'Did not process data correct in on_event'
    listener.notify(None)
    assert len(listener.calls) == 1, 'on_event triggered for None'
    assert listener.closed, "Listener did not teardown on kill event"


def test_chained_notify(bee_factory):
    listener1 = bee_factory.create('listener')
    listener2 = bee_factory.create('listener')
    listener3 = bee_factory.create('listener')
    listener1.chain(listener2).chain(listener3)
    listener1.notify(1)
    assert len(listener2.calls) == 1, 'First chained on_event did not trigger'
    assert len(listener3.calls) == 1, 'Second chained on_event did not trigger'
    assert isinstance(listener2.calls[0], pybeehive.Event), 'Did convert data to event in chained on_event'
    assert isinstance(listener3.calls[0], pybeehive.Event), 'Did convert data to event in chained on_event'
    listener1.notify(None)
    assert len(listener2.calls) == 1, 'First chained on_event triggered on None'
    assert len(listener3.calls) == 1, 'Second chained on_event triggered on None'


def test_chained_notify_conditional(bee_factory):
    listener1 = bee_factory.create('listener')
    listener2 = bee_factory.create('listener')
    listener3 = bee_factory.create('listener')
    listener1.chain(listener2).chain(listener3)

    def on_event(event):
        listener2.calls.append(event)
        if event.data == 2:
            return event

    listener2.on_event = on_event
    listener1.notify(1)
    assert len(listener2.calls) == 1, 'First chained on_event did not trigger'
    assert len(listener3.calls) == 0, 'Second chained on_event triggered'
    listener1.notify(2)
    assert len(listener2.calls) == 2, 'First chained on_event did not trigger'
    assert len(listener3.calls) == 1, 'Second chained on_event triggered on None'


def test_notify_with_exception(bee_factory):
    listener = bee_factory.create('listener', failing=True)
    listener.notify(1)
    assert len(listener.calls) == 0, 'Failed on_event added data'


def test_chained_notify_with_exception(bee_factory):
    listener1 = bee_factory.create('listener', failing=True)
    listener2 = bee_factory.create('listener', failing=True)
    listener3 = bee_factory.create('listener', failing=True)
    # Ensure both ways of calling work
    pybeehive.Listener.chain([listener1], listener2).chain(listener3)
    listener1.notify(1)
    assert len(listener2.calls) == 0, 'First chained on_event triggered'
    assert len(listener3.calls) == 0, 'Second chained on_event triggered'
