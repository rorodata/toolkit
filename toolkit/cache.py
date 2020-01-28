"""
toolkit.cache
~~~~~~~~~~~~~

Caching Utilities from the toolkit.
"""
import functools

def memoize(f):
    """Memoizes a function, caching its return values for each input.
    """
    cache = {}
    @functools.wraps(f)
    def g(*a):
        if a not in cache:
            cache[a] = f(*a)
        return cache[a]
    return g
