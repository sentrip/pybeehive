from threading import Thread
import random
import time
import pytest
import _thread

from pybeehive.socket import SocketStreamer, SocketListener
import pybeehive


def test_no_zmq(hive):
    hive._socket_listener_class = None
    hive._socket_streamer_class = None

    with pytest.raises(RuntimeError):
        hive.socket_listener(('', 0))(lambda: None)

    with pytest.raises(RuntimeError):
        hive.socket_streamer(('', 0))(lambda: None)

    hive._socket_listener_class = SocketListener
    hive._socket_streamer_class = SocketStreamer


def test_messaging(client_server):
    msg = b'data'
    client, server = client_server
    server_messages = iter(server.iter_messages())
    for i in range(5):
        client.send(msg)
        received = next(server_messages)
        assert received == msg, 'Incorrect message sent to server'


def test_socket_streamer_listener_decorator_definition(hive):
    address = '127.0.0.1', random.randint(7000, 10000)
    events = []

    @hive.socket_streamer(address)
    def stream():
        return

    @hive.socket_listener(address)
    def parse_event(event):
        event = pybeehive.Event(event.data + 1, created_at=event.created_at)
        events.append(event)
        return event

    hive.add(SocketStreamer(address))
    hive.submit_event(pybeehive.Event(-1))
    hive.run(threaded=True)
    start = time.time()
    while len(events) < 5 and time.time() - start < 2:
        time.sleep(1e-4)
    hive.close()
    assert len(events) >= 5, "Hive did not process all events"
    for i, e in enumerate(events):
        assert i == e.data, "Event data was not parsed by listener"


def test_socket_streamer_listener_loop(hive):
    address = '127.0.0.1', random.randint(7000, 10000)
    events = []

    @hive.socket_listener(address)
    def parse_event(event):
        event = pybeehive.Event(event.data + 1, created_at=event.created_at)
        events.append(event)
        return event

    hive.add(SocketStreamer(address))
    hive.submit_event(pybeehive.Event(-1))
    hive.run(threaded=True)
    start = time.time()
    while len(events) < 5 and time.time() - start < 2:
        time.sleep(1e-4)
    hive.close()
    assert len(events) >= 5, "Hive did not process all events"
    for i, e in enumerate(events):
        assert i == e.data, "Event data was not parsed by listener"


def test_multiple_listeners_single_streamer(hive):

    address = '127.0.0.1', random.randint(7000, 10000)
    events = []

    class Listener(SocketListener):
        def parse_event(self, event):
            event = pybeehive.Event(event.data + 1, created_at=event.created_at)
            events.append(event)
            return event

    hive.add(SocketStreamer(address))
    for _ in range(3):
        hive.add(Listener(address))

    hive.submit_event(pybeehive.Event(-1))
    hive.run(threaded=True)
    start = time.time()
    while len(events) < 12 and time.time() - start < 2:
        time.sleep(1e-4)
    hive.close()
    assert len(events) >= 12, "Hive did not process all events"
    for i, e in enumerate(events[:12]):
        # First three
        if i < 3:
            assert e.data == 0, "Multiple listeners sent incorrect events"
        # Last nine (each cycle is 3x)
        else:
            assert e.data == 1, "Streamer did not propagate events to listeners"


def test_message_closed_server(hive):
    address = '127.0.0.1', random.randint(7000, 10000)
    events = []

    @hive.socket_listener(address)
    def parse_event(event):
        events.append(event)
        return event

    hive.submit_event(pybeehive.Event(-1))
    hive.run(threaded=True)
    start = time.time()
    while len(events) < 1 and time.time() - start < 2:
        time.sleep(1e-4)
    hive.close()
    assert len(events) >= 1, "Hive did not process all events"


def test_interrupted_streamer_listener_loop(hive):
    address = '127.0.0.1', random.randint(7000, 10000)
    events = []

    def interrupt():
        while len(events) < 2:
            time.sleep(1e-3)
        _thread.interrupt_main()

    def parse_event(event):
        events.append(event)
        return event

    streamer = SocketStreamer(address)
    listener = SocketListener(address)
    listener.parse_event = parse_event
    hive.add(streamer)
    hive.add(listener)
    hive.submit_event(pybeehive.Event(-1))
    Thread(target=interrupt).start()
    hive.run()
    assert len(events) > 1, "Listener did not receive any events from streamer"
    assert not hive.alive, 'KeyboardInterrupt did not kill hive'
    assert not streamer.server.alive, 'KeyboardInterrupt did not kill server'
    assert not listener.client.alive, 'KeyboardInterrupt did not kill client'

