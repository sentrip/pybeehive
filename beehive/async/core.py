import asyncio
from abc import abstractmethod
from ..core import Event, Listener as SyncListener, Streamer as SyncStreamer
from .utils import AsyncContextManager


class Listener(SyncListener):
    @abstractmethod
    async def on_event(self, event):
        raise NotImplementedError

    async def notify(self, event):
        if event:
            try:
                event = await self.on_event(event)
                event = Event(event)
            except Exception as e:
                self.on_exception(e)
            await asyncio.gather(*[
                bee.notify(event) for bee in self.chained_bees
            ])

    async def setup(self):
        pass

    async def teardown(self):
        pass


class Streamer(SyncStreamer):

    _event_class = asyncio.Event

    @abstractmethod
    async def stream(self):
        raise NotImplementedError

    async def setup(self):
        pass

    async def teardown(self):
        pass

    async def run(self):
        self._assert_queue_is_set()
        while self.alive:
            try:
                async with AsyncContextManager(self.stream()) as stream:
                    async for data in stream:
                        event = Event(data, topic=self.topic)
                        await self._q.put(event)
                        # break long running streams if the kill event is set
                        if not self.alive:
                            break
            except Exception as e:
                self.on_exception(e)
