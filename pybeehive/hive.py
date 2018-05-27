import inspect
from collections import defaultdict
from contextlib import contextmanager
from queue import Queue, Empty
from threading import Thread

from .core import Listener, Streamer, Event, Killable
from .logging import create_logger, debug_handler, default_handler
try:
    from .socket import SocketListener, SocketStreamer
# This is tested, just not by patching imports
except ImportError:  # pragma: nocover
    SocketListener, SocketStreamer = None, None  # pragma: nocover


def _loop(event_queue, listeners, kill_event):
    while not kill_event.is_set():
        # Timeout on get and Empty catch ensure threads
        # are not waiting forever for an item in the queue
        try:
            event = event_queue.get(timeout=0.001)
            for bee in listeners:
                bee.notify(event)
        except Empty:
            continue
        except KeyboardInterrupt:
            break


class Hive(Killable):
    """

    """
    _listener_class = Listener
    _streamer_class = Streamer
    _socket_listener_class = SocketListener
    _socket_streamer_class = SocketStreamer

    def __init__(self):
        super(Hive, self).__init__()
        self.streamers = []
        self.listeners = _ListenerTree()
        self._event_queue = Queue()

        self.logger = create_logger(handler=default_handler)

    def add(self, *bees):
        """

        :param bees:
        """
        for bee in bees:
            try:
                assert isinstance(bee, Listener), \
                    'Bee must be an instance of Listener or Streamer'
                self.listeners.add_listener(bee)
            except AssertionError:
                assert isinstance(bee, Streamer), \
                    'Bee must be an instance of Listener or Streamer'
                bee.set_queue(self._event_queue)
                self.streamers.append(bee)

    def listener(self, chain=None, filters=None, **kwargs):
        """

        :param chain:
        :param filters:
        :param kwargs:
        :return:
        """
        # for single decorator usage 'chain' is the on_event function
        if inspect.isfunction(chain):
            self._create_listener(chain, **kwargs)
        else:
            if chain:
                self.listeners.validate_chain(chain)

            if filters:
                if not isinstance(filters, (list, set)):
                    filters = [filters]
                self.listeners.validate_filters(filters)

            def wrapper(f):
                self._create_listener(f, chain, filters, **kwargs)
            return wrapper

    def streamer(self, topic=None, **kwargs):
        """

        :param topic:
        :param kwargs:
        :return:
        """
        # for single decorator usage 'topic' is the stream function
        if inspect.isfunction(topic):
            self._create_streamer(topic, **kwargs)
        else:
            if topic:
                self.listeners.validate_filters([topic])

            def wrapper(f):
                self._create_streamer(f, topic=topic, **kwargs)
            return wrapper

    def socket_listener(self, address, chain=None, filters=None):
        """

        :param address:
        :param chain:
        :param filters:
        :return:
        """
        if self._socket_listener_class is None:
            raise RuntimeError('pyzmq required to create pybeehive sockets')

        def wrapped(f):
            return self.listener(
                chain=chain, filters=filters,
                klass=self._socket_listener_class, klass_args=(address,),
                method_name='parse_event'
            )(f)
        return wrapped

    def socket_streamer(self, address, topic=None):
        """

        :param address:
        :param topic:
        :return:
        """
        if self._socket_streamer_class is None:
            raise RuntimeError('pyzmq required to create pybeehive sockets')

        def wrapped(f):
            return self.streamer(
                topic=topic,
                klass=self._socket_streamer_class, klass_args=(address,),
                method_name=None
            )(f)
        return wrapped

    def submit_event(self, event):
        """

        :param event:
        :return:
        """
        assert isinstance(event, Event), "Can only submit Events to the Hive"
        self._event_queue.put_nowait(event)

    def run(self, threaded=False, debug=False):
        """

        :param threaded:
        :param debug:
        :return:
        """
        if debug:
            self.logger.addHandler(debug_handler)
        if threaded:
            worker = Thread(target=self._run)
            worker.start()
            return worker
        else:
            self._run()

    def close(self):
        """

        :return:
        """
        self.kill()
        for streamer in self.streamers:
            streamer.kill()

    def _wrap_stream(self, stream_func):
        return lambda s: stream_func()

    def _create_listener(self, func, chain=None, filters=None,
                         klass=None, klass_args=(), method_name='on_event'):

        _Listener = type(func.__name__, (klass or self._listener_class,), {
            method_name: lambda s, e: func(e)
        })
        self.listeners.add_listener(
            _Listener(*klass_args, filters=filters), chain=chain
        )

    def _create_streamer(self, func, topic=None,
                         klass=None, klass_args=(), method_name='stream'):
        if method_name == 'stream':
            klass_dict = {method_name: self._wrap_stream(func)}
        else:
            klass_dict = {}
        klass = klass or self._streamer_class
        _Streamer = type(func.__name__, (klass,), klass_dict)
        self.add(_Streamer(*klass_args, topic=topic))

    def _run(self):
        with self._setup_teardown_streamers():
            with self._setup_teardown_listeners():
                self.logger.info("The hive is now live!")
                try:
                    _loop(self._event_queue, self.listeners, self.kill_event)
                finally:
                    self.logger.info("Shutting down hive...")
        self.close()

    @contextmanager
    def _setup_teardown_listeners(self):
        self.listeners.call_method_recursively('setup')
        yield
        self.listeners.call_method_recursively('teardown')

    @contextmanager
    def _setup_teardown_streamers(self):
        for streamer in self.streamers:
            self._setup_streamer(streamer)

        yield

        for streamer in self.streamers:
            self._teardown_streamer(streamer)

    def _setup_streamer(self, streamer):
        try:
            streamer.setup()
        except Exception as e:
            self.logger.exception("setup %s - %s", str(streamer), repr(e))
        else:
            self.logger.debug("setup %s - OK", str(streamer))
        streamer.thread = Thread(target=streamer.run)
        streamer.thread.start()

    def _teardown_streamer(self, streamer):
        try:
            streamer.kill()
            streamer.teardown()
            streamer.thread.join()
            self.logger.debug("teardown %s - OK", str(streamer))
        except Exception as e:
            self.logger.exception("teardown %s - %s", str(streamer), repr(e))


class _ListenerTree:
    def __init__(self):
        self._listeners = defaultdict(list)
        self.logger = create_logger(name='pybeehive.hive.listeners')

    def __iter__(self):
        for v in self._listeners.values():
            for l in v:
                yield l

    def __len__(self):
        length = 0
        for _ in self:
            length += 1
        return length

    def add_listener(self, listener, chain=None):
        if chain:
            self.chain(listener, chain)
        else:
            self._listeners[listener.__class__.__name__].append(listener)

    def call_method_recursively(self, method_name, *args, **kwargs):
        results = []
        for bee in self._recursive_iter():
            try:
                result = bee.__getattribute__(method_name)(*args, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "%s %s - %s", method_name, str(bee), repr(e))
            else:
                self.logger.debug("%s %s - OK", method_name, str(bee))
                results.append(result)
        return results

    def chain(self, listener, chain):
        bees_to_chain = []

        if isinstance(chain, str):
            bees_to_chain.extend(self.listeners_by_name(chain))

        else:
            for c in chain:
                bees_to_chain.extend(self.listeners_by_name(c))

        for bee in bees_to_chain:
            bee.chain(listener)

    def listeners_by_name(self, name):
        listeners = []
        for listener in self._recursive_iter():
            if listener.__class__.__name__ == name:
                listeners.append(listener)
        return listeners

    @staticmethod
    def validate_chain(c):

        if isinstance(c, str):
            return

        elif isinstance(c, list) and all(isinstance(i, str) for i in c):
            return

        else:
            raise TypeError("chain must be a string or a list of strings")

    @staticmethod
    def validate_filters(items):
        for item in items:
            try:
                hash(item)
            except TypeError:
                raise TypeError("filter %s is not hashable" % str(item))

    def _recursive_iter(self, listeners=None, visited=None):
        visited = visited or []
        if listeners is None:
            listeners = self

        for listener in listeners:
            if listener not in visited:
                visited.append(listener)
                yield listener
            for _l in self._recursive_iter(listener.chained_bees, visited):
                if _l not in visited:
                    visited.append(_l)
                    yield _l
