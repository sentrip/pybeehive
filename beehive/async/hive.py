from contextlib import contextmanager
import asyncio
import inspect

from ..hive import Hive as SyncHive
from .core import Listener, Streamer
from .utils import AsyncGenerator
try:
    from .socket import SocketListener, SocketStreamer
except ImportError:
    SocketListener, SocketStreamer = None, None


async def _loop_async(event_queue, listeners, kill_event):
    while not kill_event.is_set():
        # This try except mimics 'await queue.get()',
        # but continuously yields control back to loop
        # in order to allow graceful shutdown
        try:
            event = event_queue.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(1e-3)
        else:
            await asyncio.gather(*[
                    bee.notify(event) for bee in listeners
                ])


class Hive(SyncHive):

    _event_class = asyncio.Event
    _listener_class = Listener
    _streamer_class = Streamer
    _socket_listener_class = SocketListener
    _socket_streamer_class = SocketStreamer

    def __init__(self):
        super(Hive, self).__init__()
        # This is set at runtime depending on the run context
        self.loop = None
        self._event_queue = asyncio.Queue()

    def _wrap_stream(self, stream_func):
        def stream(s):
            # pre-python3.6 wrapping for async function
            if inspect.iscoroutinefunction(stream_func):
                return AsyncGenerator(stream_func)
            # python3.6 or closure usage: return async generator
            return stream_func()
        return stream

    def _run(self):
        self._set_loop()
        with self._setup_teardown_streamers() as jobs:
            with self._setup_teardown_listeners():
                task = asyncio.ensure_future(asyncio.gather(
                    *jobs,
                    _loop_async(
                        self._event_queue, self.listeners, self.kill_event
                    )
                ))
                try:
                    self.logger.info("The hive is now live!")
                    self.loop.run_until_complete(task)
                except KeyboardInterrupt:
                    pass  # Need explicit catch here
                finally:
                    self.logger.info("Shutting down hive...")
        task.cancel()
        self.close()

    def _set_loop(self):
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            # Create new event loop when called from a thread
            self.loop = asyncio.get_event_loop_policy().new_event_loop()
            asyncio.set_event_loop(self.loop)

    @contextmanager
    def _setup_teardown_listeners(self):
        setup_futures = self.listeners.call_method_recursively('setup')
        if setup_futures:
            self.loop.run_until_complete(asyncio.gather(
                *setup_futures, return_exceptions=True))
        yield
        teardown_futures = self.listeners.call_method_recursively('teardown')
        if teardown_futures:
            self.loop.run_until_complete(asyncio.gather(
                *teardown_futures, return_exceptions=True))

    @contextmanager
    def _setup_teardown_streamers(self):
        jobs = self.loop.run_until_complete(asyncio.gather(
            *[self._setup_streamer(s) for s in self.streamers]
        ))
        yield jobs

        self.loop.run_until_complete(asyncio.gather(
            *[self._teardown_streamer(s) for s in self.streamers]
        ))

    async def _setup_streamer(self, streamer):
        try:
            await streamer.setup()
        except Exception as e:
            self.logger.exception("setup %s - %s", streamer, repr(e))
        else:
            self.logger.debug("setup %s - OK", streamer)
        return streamer.run()

    async def _teardown_streamer(self, streamer):
        streamer.kill()
        try:
            await streamer.teardown()
            self.logger.exception("teardown %s - OK", str(streamer))
        except Exception as e:
            self.logger.exception("teardown %s - %s", str(streamer), repr(e))
