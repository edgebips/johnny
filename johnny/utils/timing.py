"""Timing utils.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from typing import Optional
import contextlib
import time
import logging
import functools


def create_logger(logger: Optional[logging.Logger]):
    """Create a logger context manager, if the given value is not null."""
    if logger:
        return functools.partial(log_time, log_timings=logger.info, indent=1)
    else:
        return lambda operation_name: contextlib.nullcontext()


@contextlib.contextmanager
def log_time(operation_name, log_timings, indent=0):
    """A context manager that times the block and logs it to info level.

    Args:
      operation_name: A string, a label for the name of the operation.
      log_timings: A function to write log messages to. If left to None,
        no timings are written (this becomes a no-op).
      indent: An integer, the indentation level for the format of the timing
        line. This is useful if you're logging timing to a hierarchy of
        operations.
    Yields:
      The start time of the operation.
    """
    time1 = time.time()
    yield time1
    time2 = time.time()
    if log_timings:
        log_timings(
            "Operation: {:48} Time: {}{:6.0f} ms".format(
                "'{}'".format(operation_name), "      " * indent, (time2 - time1) * 1000
            )
        )
