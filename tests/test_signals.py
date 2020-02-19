from toolkit import Signal

class TestSignal:
    def test_send(self):
        signal_foo = Signal("foo")

        calls = []

        @signal_foo.connect
        def f(x):
            calls.append(['f', x])

        @signal_foo.connect
        def g(x):
            calls.append(['g', x])

        signal_foo.send(42)
        assert calls == [
            ['f', 42],
            ['g', 42]]
