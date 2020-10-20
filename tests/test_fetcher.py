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
