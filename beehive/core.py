import pickle
from abc import ABC, abstractmethod
from hashlib import md5
from threading import Event as _Event
from time import time


class Event:
    __slots__ = ['data', 'topic', 'id', 'created_at']

    def __init__(self, data, topic=None, created_at=None):
        if isinstance(data, Event):
            self.data = data.data
            self.topic = data.topic
            self.id = data.id
            self.created_at = data.created_at
        else:
            self.data = data
            self.topic = topic
            self.created_at = created_at or time()
            self.id = self.create_id(self.data, self.created_at)

    def __eq__(self, other):
        return isinstance(other, Event) \
               and self.id == other.id \
               and self.topic == other.topic

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        return 'Event(created_at={}, data={})'.format(
            int(self.created_at), str(self.data)[:100]
        )

    @staticmethod
    def create_id(data, time_created):
        return md5((str(data) + str(time_created)).encode()).hexdigest()

    @staticmethod
    def fromstring(string):
        return pickle.loads(string)

    def tostring(self):
        return pickle.dumps(self)


class Listener(ABC):
    def __init__(self, filters=None):
        super(Listener, self).__init__()
        self.chained_bees = []
        self.filters = set(filters or [])

    def __str__(self):
        return "%s(filters=%s)" % (self.__class__.__name__, self.filters)

    def chain(self, bee, *bees):
        # static usage with list for many-to-one
        if isinstance(self, list):
            for other in self:
                other.chain(bee)
        else:
            self.chained_bees.append(bee)
            for other in bees:
                self.chained_bees.append(other)
        return bee

    def filter(self, event):
        # If no filters are defined then listens to all events
        return not self.filters or event.topic in self.filters

    def notify(self, event):
        if event and self.filter(event):
            try:
                event = Event(self.on_event(event))
            except Exception as e:
                self.on_exception(e)
            # Only propagate events that this listener can accept
            for bee in self.chained_bees:
                bee.notify(event)

        if not event:
            for bee in self.chained_bees:
                bee.notify(event)
            self.teardown()

    @abstractmethod
    def on_event(self, event):
        raise NotImplementedError

    def on_exception(self, exception):
        pass

    def setup(self):
        pass

    def teardown(self):
        pass


class Killable:
    _event_class = _Event

    def __init__(self):
        self.kill_event = self._event_class()

    @property
    def alive(self):
        return not self.kill_event.is_set()

    def kill(self):
        self.kill_event.set()


class Streamer(Killable, ABC):
    def __init__(self, topic=None):
        super(Streamer, self).__init__()
        self.topic = topic
        self._q = None  # this is set when a streamer is added to a hive

    def __str__(self):
        return "%s(topic=%s)" % (self.__class__.__name__, self.topic)

    @abstractmethod
    def stream(self):
        raise NotImplementedError

    def on_exception(self, exception):
        pass

    def setup(self):
        pass

    def teardown(self):
        pass

    def set_queue(self, q):
        self._q = q

    def run(self):
        self._assert_queue_is_set()
        stream = self.stream()
        while self.alive:
            try:
                event = Event(next(stream), topic=self.topic)
            except StopIteration:
                stream = self.stream()
            except Exception as e:
                self.on_exception(e)
            else:
                self._q.put(event)

    def _assert_queue_is_set(self):
        assert self._q is not None, \
            "You must first set the output queue with " \
            "Streamer.set_queue before running a Streamer"
