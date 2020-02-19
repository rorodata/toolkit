"""
toolkit.signals
~~~~~~~~~~~~~~~

The signal system allows parts of an application to get notified
when events occur elsewhere in the application, without a tight coupling.
"""

class Signal:
    def __init__(self, name=None):
        """Creates a signal.
        """
        self.name = name or str(id(self))
        self.listeners = []

    def send(self, *args, **kwargs):
        """Notifies all listeners of this signal.
        """
        for f in self.listeners:
            f(*args, **kwargs)

    def connect(self, f):
        """Connects a function to this signal.

        Can also be used like a decorator.

        @signal_login.connect
        def on_login(username):
            ...
        """
        self.listeners.append(f)
        return f

    def __repr__(self):
        return "<Signal:{}>".format(self.name)
