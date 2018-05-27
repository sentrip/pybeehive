from threading import Thread
import asyncio
import random
import time
import pytest
import _thread

from pybeehive.async.socket import SocketListener, SocketStreamer
import pybeehive


def test_no_zmq(async_hive):
    async_hive._socket_listener_class = None
    async_hive._socket_streamer_class = None

    with pytest.raises(RuntimeError):
        async_hive.socket_listener(('', 0))(lambda: None)

    with pytest.raises(RuntimeError):
        async_hive.socket_streamer(('', 0))(lambda: None)

    async_hive._socket_listener_class = SocketListener
    async_hive._socket_streamer_class = SocketStreamer


# If run_in_new_loop is not the first argument, things break
def test_messaging(run_in_new_loop, async_client_server):
    client, server = async_client_server

    async def _test():
        msg = b'data'
        generator = server.iter_messages().__aiter__()
        for _ in range(5):
            await client.send(msg)
            received = await generator.__anext__()
            assert received == msg, 'Incorrect message sent to server'
        await client.shutdown()
        await server.shutdown()

    run_in_new_loop(_test)


def test_socket_streamer_listener_loop(async_hive):
    address = '127.0.0.1', random.randint(7000, 10000)
    events = []

    @async_hive.socket_listener(address)
    async def parse_event(event):
        event = pybeehive.Event(event.data + 1, created_at=event.created_at)
        events.append(event)
        return event

    async_hive.add(SocketStreamer(address))
    async_hive.submit_event(pybeehive.Event(-1))
    async_hive.run(threaded=True)
    start = time.time()
    while len(events) < 5 and time.time() - start < 2:
        time.sleep(1e-4)
    async_hive.close()
    assert len(events) >= 5, "Hive did not process all events"
    for i, e in enumerate(events):
        assert i == e.data, "Event data was not parsed by listener"


def test_multiple_listeners_single_streamer(async_hive):
    address = '127.0.0.1', random.randint(7000, 10000)
    events = []

    class Listener(SocketListener):
        async def parse_event(self, event):
            event = pybeehive.Event(event.data + 1, created_at=event.created_at)
            events.append(event)
            return event

    async_hive.add(SocketStreamer(address))
    for _ in range(3):
        async_hive.add(Listener(address))

    async_hive.submit_event(pybeehive.Event(-1))
    async_hive.run(threaded=True)
    start = time.time()
    while len(events) < 12 and time.time() - start < 2:
        time.sleep(1e-4)
    async_hive.close()
    assert len(events) >= 12, "Hive did not process all events"
    for i, e in enumerate(events[:12]):
        # First three
        if i < 3:
            assert e.data == 0, "Multiple listeners sent incorrect events"
        # Last nine (each cycle is 3x)
        else:
            assert e.data == 1, "Streamer did not propagate events to listeners"


def test_message_closed_server(async_hive):
    address = '127.0.0.1', random.randint(7000, 10000)
    events = []

    @async_hive.socket_listener(address)
    async def parse_event(event):
        events.append(event)
        return event

    async_hive.submit_event(pybeehive.Event(-1))
    async_hive.run(threaded=True)
    start = time.time()
    while len(events) < 1 and time.time() - start < 2:
        time.sleep(1e-4)
    async_hive.kill()
    assert len(events) >= 1, "Hive did not process all events"


# This test can pass, but due to the inconsistent handling of
# KeyboardInterrupt in zmq.asyncio.Socket.send it does not
# pass consistently and is thus skipped in the normal tests.
# If pyzmq is updated to fix this the skip can be removed.
@pytest.mark.skip
def test_interrupted_streamer_listener_loop(async_hive):
    address = '127.0.0.1', random.randint(7000, 10000)
    events = []

    def interrupt():
        time.sleep(1e-2)
        _thread.interrupt_main()

    async def parse_event(event):
        await asyncio.sleep(1e-4)
        events.append(event)
        return event

    streamer = SocketStreamer(address)
    listener = SocketListener(address)
    listener.parse_event = parse_event
    async_hive.add(streamer)
    async_hive.add(listener)
    async_hive.submit_event(pybeehive.Event(-1))
    Thread(target=interrupt).start()
    async_hive.run()
    assert len(events) > 1, "Listener did not receive any events from streamer"
    assert not async_hive.alive, 'KeyboardInterrupt did not kill hive'
    assert not streamer.server.alive, 'KeyboardInterrupt did not kill server'
    assert not listener.client.alive, 'KeyboardInterrupt did not kill client'
