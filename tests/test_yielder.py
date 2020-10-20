import time
import random
import scheduled
# from scheduled import RedisQueue, Distributer, KeyYielder

class KeyYielder(scheduled.KeyYielder):

    def yield_key(self):
        for i in range(100):
            yield i


def test_basic():
    yielder = KeyYielder()

    for key in yielder.yield_key():
        print(key)


def test_basic2():
    yielder = KeyYielder()

    fn = yielder.yield_key()
    for key in fn:
        print(key)


def test_basic3():
    yielder = KeyYielder()

    fn = yielder.yield_key()
    print(next(fn))
    print(next(fn))


if 0:
    test_basic()
if 0:
    test_basic2()
if 1:
    test_basic3()
