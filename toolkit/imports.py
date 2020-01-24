"""
toolkit.imports
~~~~~~~~~~~~~~~

Import utilities from the toolkit.
"""

class LazyModule:
    def __init__(self, name):
        self.__dict__['_name'] = name
        self.__dict__['_mod'] = None

    def load(self):
        if self._mod is None:
            import importlib
            self.__dict__['_mod'] = importlib.import_module(self._name)
        return self._mod

    def __getattr__(self, name):
        return getattr(self.load(), name)

    def __setattr__(self, name, value):
        return setattr(self.load(), name, value)

    def __delattr__(self, name):
        return delattr(self.load(), name)

    def __repr__(self):
        return "<LazyModule: {}>".format(self._name)

def lazy_import(module_name):
    """Imports a module lazily.

    The module is loaded when an attribute it is accessed from that module for the first time.

        pd = lazy_import("pandas")

    This only works with absolute imports and does not work with relative imports.
    """
    return LazyModule(module_name)
