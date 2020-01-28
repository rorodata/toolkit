"""
toolkit.api
~~~~~~~~~~~

Utilities for Developers.
"""
from contextlib import contextmanager
import time

@contextmanager
def timeit(label):
    """Times execution of a block of code.

    USAGE:
        with timeit("importing pandas"):
            import pandas

    Output:
        importing pandas: 0.913s
    """
    t0 = time.time()
    yield
    t1 = time.time()
    print("{}: {:.3f}".format(label, t1-t0))
