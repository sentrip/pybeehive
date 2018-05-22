from queue import Empty, Queue
from threading import Thread
import time
from .core import Streamer, Listener, Event, Killable
try:
    import zmq
except ImportError:
    raise ImportError('pyzmq required for beehive.socket/beehive.async.socket')


class Server(Killable):
    def __init__(self, address):
        super(Server, self).__init__()
        self.address = address
        self.queue = Queue()
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PULL)
        self._listener_thread = None

    def _receive_into_queue(self):
        while self.alive:
            try:
                data = self.socket.recv(flags=zmq.NOBLOCK)
            except zmq.error.ZMQError:
                time.sleep(1e-6)
            else:
                self.queue.put(data)

    def iter_messages(self):
        while not self.kill_event.is_set():
            try:
                try:
                    msg = self.queue.get(timeout=0.001)
                    yield msg
                except Empty:
                    continue
            except KeyboardInterrupt:
                self.kill_event.set()

    def start(self):
        self.socket.bind("tcp://%s:%s" % self.address)
        self._listener_thread = Thread(target=self._receive_into_queue)
        self._listener_thread.start()

    def shutdown(self):
        self.kill()
        self._listener_thread.join()
        self.socket.close(linger=0)


class Client(Killable):
    def __init__(self, address):
        super(Client, self).__init__()
        self.address = address
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)

    def send(self, data):
        while self.alive:
            try:
                self.socket.send(data, flags=zmq.constants.NOBLOCK)
                return
            except zmq.error.ZMQError:
                time.sleep(1e-6)

    def connect(self):
        self.socket.connect('tcp://%s:%s' % self.address)

    def shutdown(self):
        self.kill_event.set()
        self.socket.close(linger=0)


class SocketStreamer(Streamer):
    def __init__(self, address, topic=None):
        super(SocketStreamer, self).__init__(topic=topic)
        self.server = Server(address)

    def setup(self):
        self.server.start()

    def teardown(self):
        self.server.shutdown()

    def stream(self):
        for msg in self.server.iter_messages():
            event = Event.fromstring(msg)
            yield Event(
                event.data, topic=self.topic, created_at=event.created_at
            )


class SocketListener(Listener):
    def __init__(self, address, filters=None):
        super(SocketListener, self).__init__(filters=filters)
        self.client = Client(address)

    def setup(self):
        self.client.connect()

    def teardown(self):
        self.client.shutdown()

    def on_event(self, event):
        self.client.send(self.parse_event(event).tostring())

    def parse_event(self, event):
        return event
