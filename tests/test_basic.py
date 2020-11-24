import config
import pytest
import scheduled


def test_fetcher():
    class Fetcher(scheduled.Fetcher):
        def fetch(self, key):
            print('fetch ..')
            return key

    f = Fetcher()
    assert f.fetch('key') == 'key'


def test_fetcher2():
    class Fetcher(scheduled.Fetcher):
        def fetch(self, key):
            raise Exception(key)

    f = Fetcher()
    with pytest.raises(Exception):
        f.fetch('key')


class KeyYielder(scheduled.KeyYielder):

    def yield_key(self):
        for i in range(100):
            yield i


def test_basic():
    yielder = KeyYielder()
    for i, key in enumerate(yielder.yield_key()):
        assert(i == key)


def test_basic2():
    yielder = KeyYielder()
    fn = yielder.yield_key()
    for i, key in enumerate(fn):
        assert(i == key)


def test_basic3():
    yielder = KeyYielder()
    fn = yielder.yield_key()
    assert(next(fn) == 0)
    assert(next(fn) == 1)
