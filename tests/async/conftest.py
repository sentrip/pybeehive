import asyncio
import random
import pytest
import sys, os
sys.path.append(os.path.abspath('../beehive'))
import beehive.async
import beehive.async.socket
from beehive.async.utils import async_generator


@pytest.fixture
def run_in_loop():
    def wrapped(f, *args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(f(*args, **kwargs))
    return wrapped


@pytest.fixture
def run_in_new_loop(request):
    old_loop = asyncio.get_event_loop()
    loop = asyncio.get_event_loop_policy().new_event_loop()
    asyncio.set_event_loop(loop)
    request.addfinalizer(loop.close)

    def wrapped(f, *args, **kwargs):
        result = loop.run_until_complete(f(*args, **kwargs))
        asyncio.set_event_loop(old_loop)
        return result

    return wrapped


# Listener / Streamer
# ===================

class AsyncTestListener(beehive.async.Listener):
    def __init__(self):
        super(AsyncTestListener, self).__init__()
        self.calls = []

    async def failed_on_event(self, event):
        raise Exception

    async def on_event(self, event):
        self.calls.append(event)
        await asyncio.sleep(0)
        return event

    async def multiprocess_on_event(self, event):
        with open(str(event.data) + '.txt', 'w') as f:
            f.write('success')
        await asyncio.sleep(0)


class AsyncTestStreamer(beehive.async.Streamer):
    def __init__(self):
        super(AsyncTestStreamer, self).__init__()
        self.count = 0
        self.ex = False
        self.i = 0

    @async_generator
    async def stream(self):
        if self.i < 10:
            self.i += 1
            self.count += 1
            await asyncio.sleep(0)
            return self.i - 1
        else:
            self.kill()
            raise StopAsyncIteration

    @async_generator
    async def failed_stream(self):
        if self.i < 3:
            self.i += 1
            await asyncio.sleep(0)
            return self.i - 1
        else:
            self.kill()
            raise Exception

    def on_exception(self, exception):
        self.ex = True


class PrePython36Streamer(beehive.async.Streamer):
    def __init__(self):
        super(PrePython36Streamer, self).__init__()
        self.i = 0

    @beehive.async.async_generator
    async def stream(self):
        self.i += 1
        if self.i <= 12:
            return self.i
        else:
            raise StopAsyncIteration


# Hive and bee creation
# =====================

@pytest.fixture
def async_bee_factory(request):
    class BeeFactory(object):
        @staticmethod
        def create(_type, failing=False):
            if _type == 'streamer':
                bee = AsyncTestStreamer()
                if failing:
                    bee.stream = bee.failed_stream
            elif _type == 'listener':
                bee = AsyncTestListener()
                if failing:
                    bee.on_event = bee.failed_on_event
            else:
                bee = PrePython36Streamer()
            return bee
    return BeeFactory()


@pytest.fixture
def async_hive():
    return beehive.async.Hive()


# Socket client/server
# ====================

@pytest.fixture
def async_client_server():
    loop = asyncio.get_event_loop()
    port = random.randint(7000, 10000)
    server = beehive.async.socket.Server(('127.0.0.1', port))
    client = beehive.async.socket.Client(('127.0.0.1', port))
    loop.run_until_complete(server.start())
    loop.run_until_complete(client.connect())
    return client, server


