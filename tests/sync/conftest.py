import pytest
import beehive


# Listener / Streamer
# ===================

class TestListener(beehive.Listener):
    def __init__(self, filters=None):
        super(TestListener, self).__init__(filters=filters)
        self.calls = []
        self.closed = False

    def failed_on_event(self, event):
        raise Exception

    def on_event(self, event):
        self.calls.append(event)
        return event

    def multiprocess_on_event(self, event):
        with open(str(event.data) + '.txt', 'w') as f:
            f.write('success')

    def teardown(self):
        self.closed = True


class TestStreamer(beehive.Streamer):
    def __init__(self, topic=None):
        super(TestStreamer, self).__init__(topic=topic)
        self.count = 0
        self.ex = False

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
    return beehive.Hive()
