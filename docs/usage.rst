=====
Usage
=====
Synchronous:

.. code-block:: python

    from pybeehive import Hive
    import time
    hive = Hive()

    @hive.streamer
    def stream():
        while True:
            time.sleep(1)
            yield 'hello world!'

    @hive.listener
    def on_event(event):
        print(event)

    if __name__ == '__main__':
        hive.run()


.. code-block:: text

    $ python hello.py
    Event(created_at=1525400000, data="hello world!")
    Event(created_at=1525400001, data="hello world!")
    Event(created_at=1525400002, data="hello world!")
    ...


Asynchronous:

.. code-block:: python

    from pybeehive.asyn import Hive
    import asyncio
    hive = Hive()

    @hive.streamer
    async def stream():
        while True:
            await asyncio.sleep(1)
            yield 'hello world!'

    @hive.listener
    async def on_event(event):
        print(event)

    if __name__ == '__main__':
        hive.run()


.. code-block:: text

    $ python hello.py
    Event(created_at=1525400000, data="hello world!")
    Event(created_at=1525400001, data="hello world!")
    Event(created_at=1525400002, data="hello world!")
    ...
