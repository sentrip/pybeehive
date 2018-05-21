import asyncio
import inspect
from contextlib import contextmanager

from ..hive import Hive as SyncHive
from .core import Listener, Streamer
from .socket import SocketListener, SocketStreamer
from .utils import AsyncGenerator


async def _loop_async(event_queue, listeners, kill_event):
    while not kill_event.is_set():
        try:
            # This try except mimics 'await queue.get()',
            # but continuously yields control back to loop
            # in order to allow graceful shutdown
            try:
                event = event_queue.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(0)
                continue
            else:
                await asyncio.gather(*[
                    bee.notify(event) for bee in listeners
                ])
        except KeyboardInterrupt:
            break


class Hive(SyncHive):

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
                futures = [asyncio.ensure_future(j) for j in jobs]
                try:
                    self.loop.run_until_complete(_loop_async(
                        self._event_queue, self.listeners, self.kill_event
                    ))
                    self.logger.info("The Hive is now live!")

                    # Wait for streamer futures created in setup
                    if futures:
                        self.loop.run_until_complete(asyncio.wait(futures))
                except KeyboardInterrupt:  # Need explicit catch here
                    self.logger.debug("Shutting down hive...")
                    for future in futures:
                        if not future.done():
                            future.cancel()

    def _set_loop(self):
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            # Create new event loop when called from a thread
            self.loop = asyncio.get_event_loop_policy().new_event_loop()
            asyncio.set_event_loop(self.loop)

    @contextmanager
    def _setup_teardown_listeners(self):
        futures = self.listeners.call_method_recursively('setup')
        self.loop.run_until_complete(asyncio.gather(*futures))
        self.logger.debug("Initialized %d listener(s)", len(self.listeners))
        yield
        futures = self.listeners.call_method_recursively('teardown')
        self.loop.run_until_complete(asyncio.gather(*futures))

    @contextmanager
    def _setup_teardown_streamers(self):
        all_jobs = self.loop.run_until_complete(asyncio.gather(*[
            self._setup_streamer(s) for s in self.streamers
        ]))

        yield list(filter(lambda j: False if j is None else True, all_jobs))

        self.loop.run_until_complete(asyncio.gather(*[
            self._teardown_streamer(s) for s in self.streamers
        ]))

    async def _setup_streamer(self, streamer):
        try:
            await streamer.setup()
        except Exception as e:
            self.logger.exception("Setting up %s - %s", streamer, repr(e))
        else:
            self.logger.debug("Setting up %s - OK", streamer)
            return streamer.run()

    async def _teardown_streamer(self, streamer):
        try:
            streamer.kill()
            await streamer.teardown()
        except Exception as e:
            self.logger.exception("Tearing down %s - %s", streamer, repr(e))
