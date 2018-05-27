from threading import Event as _Event
import pytest
import random
import sys, os
sys.path.append(os.path.abspath('../pybeehive'))
import pybeehive
import pybeehive.socket


# Listener / Streamer
# ===================

class TestListener(pybeehive.Listener):
    def __init__(self, filters=None):
        super(TestListener, self).__init__(filters=filters)
        self.calls = []
        self.closed = False
        self.setup_event = _Event()
        self.teardown_event = _Event()

    def setup(self):
        self.setup_event.set()

    def teardown(self):
        self.closed = True
        self.teardown_event.set()

    def failed_on_event(self, event):
        raise Exception

    def on_event(self, event):
        self.calls.append(event)
        return event


class TestStreamer(pybeehive.Streamer):
    def __init__(self, topic=None):
        super(TestStreamer, self).__init__(topic=topic)
        self.count = 0
        self.ex = False
        self.setup_event = _Event()
        self.teardown_event = _Event()

    def setup(self):
        self.setup_event.set()

    def teardown(self):
        self.teardown_event.set()

    def stream(self):
        for i in range(10):
            self.count += 1
            yield i
        self.kill()

    def failed_stream(self):
        for i in range(3):
            yield i
        self.kill()
        raise Exception

    def on_exception(self, exception):
        self.ex = True


# Hive and bee creation
# =====================

@pytest.fixture
def bee_factory(request):
    class BeeFactory(object):
        @staticmethod
        def create(_type, failing=False, *args, **kwargs):
            if _type == 'streamer':
                bee = TestStreamer(*args, **kwargs)
                if failing:
                    bee.stream = bee.failed_stream
            else:
                bee = TestListener(*args, **kwargs)
                if failing:
                    bee.on_event = bee.failed_on_event
            request.addfinalizer(bee.teardown)
            return bee
    return BeeFactory()


@pytest.fixture
def hive():
    return pybeehive.Hive()


@pytest.fixture
def client_server(request):
    port = random.randint(7000, 10000)
    server = pybeehive.socket.Server(('127.0.0.1', port))
    client = pybeehive.socket.Client(('127.0.0.1', port))
    server.start()
    client.connect()
    request.addfinalizer(lambda: (client.shutdown(), server.shutdown()))
    return client, server
