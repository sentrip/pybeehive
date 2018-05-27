import logging
import sys

debug_handler = logging.StreamHandler(sys.stderr)
debug_handler.setFormatter(
    logging.Formatter(
        fmt='[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
        datefmt='%y-%y-%d %H:%M:%S'
    )
)

default_handler = logging.NullHandler()


def create_logger(name='pybeehive.hive', handler=None):
    logger = logging.getLogger(name)
    if name == 'pybeehive.hive':
        logger.propagate = False
    # set handler only for logger in top level module
    if handler:
        logger.addHandler(handler)
    return logger
