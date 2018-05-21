import random
import time

from beehive.socket import SocketStreamer, SocketListener
import beehive
import pytest


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
        event = beehive.Event(event.data + 1, created_at=event.created_at)
        events.append(event)
        return event

    hive.add(SocketStreamer(address))
    hive.submit_event(beehive.Event(-1))
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
        event = beehive.Event(event.data + 1, created_at=event.created_at)
        events.append(event)
        return event

    hive.add(SocketStreamer(address))
    hive.submit_event(beehive.Event(-1))
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
            event = beehive.Event(event.data + 1, created_at=event.created_at)
            events.append(event)
            return event

    hive.add(SocketStreamer(address))
    for _ in range(3):
        hive.add(Listener(address))

    hive.submit_event(beehive.Event(-1))
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
        event = beehive.Event(event.data + 1, created_at=event.created_at)
        events.append(event)
        return event

    # The test here is that no errors are raised when
    # messages are sent to a non-existent server
    # and the client does not block on exit
    hive.submit_event(beehive.Event(-1))
    hive.run(threaded=True)
    start = time.time()
    while len(events) < 1 and time.time() - start < 2:
        time.sleep(1e-4)
    assert len(events) >= 1, "Hive did not process all events"
    hive.kill()
