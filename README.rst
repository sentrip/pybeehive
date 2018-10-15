=========
pybeehive
=========


.. image:: https://img.shields.io/pypi/v/pybeehive.svg
    :target: https://pypi.python.org/pypi/pybeehive

.. image:: https://img.shields.io/travis/sentrip/pybeehive.svg
    :target: https://travis-ci.com/sentrip/pybeehive

.. image:: https://readthedocs.org/projects/pybeehive/badge/?version=latest
    :target: https://pybeehive.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status


.. image:: https://codecov.io/gh/sentrip/pybeehive/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/sentrip/pybeehive

.. image:: https://pyup.io/repos/github/sentrip/pybeehive/shield.svg
    :target: https://pyup.io/repos/github/sentrip/pybeehive/
    :alt: Updates



A lightweight, event-driven concurrency library with bees!


* Free software: `MIT License <https://github.com/sentrip/pybeehive/blob/master/LICENSE>`_
* Documentation: https://pybeehive.readthedocs.io.


Features
--------

* One interface for writing concurrent code, both sync and async

Basic Usage
-----------
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
