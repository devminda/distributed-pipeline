# pipeline/utils.py

import time
import logging
import functools
from contextlib import contextmanager

from pipeline.scheduler import Scheduler

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(name)s] %(levelname)s %(message)s"
)

log = logging.getLogger("pipeline")


def traced(func):
    """
    A decorator that logs entry, exit, and timing of any function.

    Decorators are functions that wrap other functions.
    This one adds logging behaviour to whatever function
    you put it on — without touching that function's code.

    Usage:
        @traced
        def my_function():
            ...

    This is equivalent to:
        my_function = traced(my_function)

    Spark equivalent: Spark internally traces every stage
    execution with metrics. This is our lightweight version
    of that observability layer.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        log.info(f"starting  {func.__qualname__}()")
        start = time.perf_counter()

        result = func(*args, **kwargs)

        elapsed = round((time.perf_counter() - start) * 1000, 2)
        log.info(f"completed {func.__qualname__}() in {elapsed}ms")

        return result

    return wrapper


@contextmanager
def pipeline_context(num_workers: int = 4):
    """
    A context manager that handles Scheduler lifecycle.

    Context managers are objects that manage resources —
    they guarantee setup happens before your code runs
    and teardown happens after, even if an exception occurs.

    The @contextmanager decorator lets you write this
    using a generator function with yield instead of
    writing a full class with __enter__ and __exit__.

    Usage:
        with pipeline_context(num_workers=4) as ctx:
            result = DataSet(data, context=ctx).filter(...).collect()
        # scheduler is always stopped here, even if collect() crashed

    Spark equivalent: SparkSession is managed the same way:
        with SparkSession.builder.getOrCreate() as spark:
            df = spark.read.csv(...)

    Args:
        num_workers : how many parallel worker processes to use

    Yields:
        dict with "scheduler" key — passed into DataSet as context
    """
    scheduler = Scheduler(num_workers=num_workers)
    scheduler.start()
    log.info(f"pipeline_context: started {num_workers} workers")

    try:
        yield {"scheduler": scheduler}
    finally:
        scheduler.stop()
        log.info("pipeline_context: all workers stopped")