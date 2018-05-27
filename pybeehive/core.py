import pickle
from abc import ABC, abstractmethod
from threading import Event as _Event
from time import time


class Event:
    """

    :param data:
    :param topic:
    :param created_at:
    """

    def __init__(self, data, topic=None, created_at=None):
        if isinstance(data, Event):
            self.data = data.data
            self.topic = topic or data.topic
            if created_at:
                self.created_at = created_at
                self.id = self.create_id(self.data, created_at)
            else:
                self.created_at = data.created_at
                self.id = data.id
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
        return self.id

    def __str__(self):
        return 'Event(created_at={}, data={})'.format(
            int(self.created_at), str(self.data)[:100]
        )

    @staticmethod
    def create_id(data, time_created):
        """

        :param data:
        :param time_created:
        :return:
        """
        return hash(str(data) + str(time_created))

    @staticmethod
    def fromstring(string):
        """

        :param string:
        :return:
        """
        return pickle.loads(string)

    def tostring(self):
        """

        :return:
        """
        return pickle.dumps(self)


class Listener(ABC):
    """

    :param filters:
    """
    def __init__(self, filters=None):
        super(Listener, self).__init__()
        self.chained_bees = []
        self.filters = set(filters or [])

    def __str__(self):
        return "%s(filters=%s)" % (self.__class__.__name__, self.filters)

    def chain(self, bee):
        """

        :param bee:
        :return:
        """
        # static usage with list for many-to-one
        if isinstance(self, list):
            for other in self:
                other.chain(bee)
        else:
            self.chained_bees.append(bee)
        return bee

    def filter(self, event):
        """

        :param event:
        :return:
        """
        # If no filters are defined then listens to all events
        return not self.filters or event.topic in self.filters

    def notify(self, event):
        """

        :param event:
        :return:
        """
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
        """

        :param event:
        :return:
        """
        raise NotImplementedError  # pragma: nocover

    def on_exception(self, exception):
        """

        :param exception:
        """
        pass

    def setup(self):
        """

        """
        pass

    def teardown(self):
        """

        """
        pass


class Killable:
    """

    """
    _event_class = _Event

    def __init__(self):
        self.kill_event = self._event_class()

    @property
    def alive(self):
        """

        :return:
        """
        return not self.kill_event.is_set()

    def kill(self):
        """

        """
        self.kill_event.set()


class Streamer(Killable, ABC):
    """

    :param topic:
    """
    def __init__(self, topic=None):
        super(Streamer, self).__init__()
        self.topic = topic
        self._q = None  # this is set when a streamer is added to a hive

    def __str__(self):
        return "%s(topic=%s)" % (self.__class__.__name__, self.topic)

    @abstractmethod
    def stream(self):
        """

        :return:
        """
        raise NotImplementedError  # pragma: nocover

    def on_exception(self, exception):
        """

        :param exception:
        """
        pass

    def setup(self):
        """

        """
        pass

    def teardown(self):
        """

        """
        pass

    def set_queue(self, q):
        """

        :param q:
        """
        self._q = q

    def run(self):
        """

        """
        self._assert_queue_is_set()
        while self.alive:
            try:
                for data in self.stream():
                    self._q.put(Event(data, topic=self.topic))
                    # break long running streams if the kill event is set
                    if not self.alive:
                        break
            except Exception as e:
                self.on_exception(e)

    def _assert_queue_is_set(self):
        assert self._q is not None, \
            "You must first set the output queue with " \
            "Streamer.set_queue before running a Streamer"
