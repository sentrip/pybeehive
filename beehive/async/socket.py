from zmq.asyncio import Context, Poller
import asyncio
import zmq

from ..core import Event, Killable
from .core import Streamer, Listener
from .utils import AsyncGenerator


class Server(Killable):

    _event_class = asyncio.Event

    def __init__(self, address):
        super(Server, self).__init__()
        self.address = address
        self.queue = asyncio.Queue()

        self.context = Context.instance()
        self.socket = self.context.socket(zmq.PULL)
        self.poller = Poller()
        self._listen_future = None

    async def _receive_into_queue(self):
        while self.alive:
            try:
                events = await self.poller.poll(timeout=1e-4)
                if self.socket in dict(events):
                    data = await self.socket.recv()
                    await self.queue.put(data)
            except zmq.error.ZMQError:
                await asyncio.sleep(1e-4)

    async def start(self):
        self.socket.bind('tcp://%s:%s' % self.address)
        self.poller.register(self.socket, zmq.POLLIN)
        self._listen_future = asyncio.ensure_future(self._receive_into_queue())
        await asyncio.sleep(0)

    async def shutdown(self):
        self.kill()
        self.poller.unregister(self.socket)
        if self._listen_future is not None and not self._listen_future.done():
            self._listen_future.cancel()
        self.socket.close(linger=0)
        await asyncio.sleep(0)

    def iter_messages(self):

        async def wrapped():
            while self.alive:
                try:
                    result = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    await asyncio.sleep(1e-6)
                else:
                    await asyncio.sleep(0)
                    return result
        return AsyncGenerator(wrapped)


class Client(Killable):

    _event_class = asyncio.Event

    def __init__(self, address):
        super(Client, self).__init__()
        self.address = address
        self.context = Context.instance()
        self.socket = self.context.socket(zmq.PUSH)

    async def connect(self):
        self.socket.connect('tcp://%s:%s' % self.address)
        await asyncio.sleep(0)

    async def send(self, data):
        while self.alive:
            return await self.socket.send(data, flags=zmq.NOBLOCK)

    async def shutdown(self):
        self.kill()
        self.socket.close(linger=0)
        await asyncio.sleep(0)


class SocketStreamer(Streamer):
    def __init__(self, address, topic=None):
        super(SocketStreamer, self).__init__(topic=topic)
        self.server = Server(address)
        self.server.kill_event = self.kill_event

    async def setup(self):
        await self.server.start()

    async def teardown(self):
        await self.server.shutdown()

    def stream(self):
        gen = self.server.iter_messages().__aiter__()

        async def wrapped():
            if self.alive:
                msg = await gen.__anext__()
                event = Event.fromstring(msg)
                return Event(
                    event.data, topic=self.topic, created_at=event.created_at
                )

        return AsyncGenerator(wrapped)


class SocketListener(Listener):
    def __init__(self, address, filters=None):
        super(SocketListener, self).__init__(filters=filters)
        self.client = Client(address)

    async def setup(self):
        await self.client.connect()

    async def teardown(self):
        await self.client.shutdown()

    async def on_event(self, event):
        event = await self.parse_event(event)
        await self.client.send(event.tostring())

    async def parse_event(self, event):
        return event
