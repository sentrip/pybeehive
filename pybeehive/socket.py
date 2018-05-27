from queue import Empty, Queue
from threading import Thread
import time
import zmq
from .core import Streamer, Listener, Event, Killable


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
                msg = self.queue.get(timeout=0.001)
                yield msg
            except Empty:
                continue

    def start(self):
        self.socket.bind("tcp://%s:%s" % self.address)
        self._listener_thread = Thread(target=self._receive_into_queue)
        self._listener_thread.start()

    def shutdown(self):
        self.kill()
        try:
            self._listener_thread.join()
        except AttributeError:
            pass  # there was an error in start, so _listener_thread is None
        self.socket.close(linger=0)


class Client(Killable):
    def __init__(self, address):
        super(Client, self).__init__()
        self.address = address
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)

    def send(self, data):
        while self.alive:
            return self.socket.send(data, flags=zmq.NOBLOCK)

    def connect(self):
        self.socket.connect('tcp://%s:%s' % self.address)

    def shutdown(self):
        self.kill_event.set()
        self.socket.close(linger=0)


class SocketStreamer(Streamer):
    """

    :param address:
    :param topic:
    """
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
    """

    :param address:
    :param filters:
    """
    def __init__(self, address, filters=None):
        super(SocketListener, self).__init__(filters=filters)
        self.client = Client(address)

    def setup(self):
        self.client.connect()

    def teardown(self):
        self.client.shutdown()

    def on_event(self, event):
        result = self.parse_event(event)
        self.client.send(result.tostring())

    def parse_event(self, event):
        """

        :param event:
        :return:
        """
        return event  # pragma: nocover
