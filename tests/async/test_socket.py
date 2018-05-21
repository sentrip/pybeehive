import random
import time
import pytest
import beehive
from beehive.async.socket import SocketListener, SocketStreamer


# If run_in_new_loop is not the first argument things break
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
        event = beehive.Event(event.data + 1, created_at=event.created_at)
        events.append(event)
        return event

    async_hive.add(SocketStreamer(address))
    async_hive.submit_event(beehive.Event(-1))
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
            event = beehive.Event(event.data + 1, created_at=event.created_at)
            events.append(event)
            return event

    async_hive.add(SocketStreamer(address))
    for _ in range(3):
        async_hive.add(Listener(address))

    async_hive.submit_event(beehive.Event(-1))
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
        event = beehive.Event(event.data + 1, created_at=event.created_at)
        events.append(event)
        return event

    # The test here is that no errors are raised when
    # messages are sent to a non-existent server
    # and the client does not block on exit
    async_hive.submit_event(beehive.Event(-1))
    async_hive.run(threaded=True)
    start = time.time()
    while len(events) < 1 and time.time() - start < 2:
        time.sleep(1e-4)
    assert len(events) >= 1, "Hive did not process all events"
    async_hive.kill()
