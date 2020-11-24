import config
import scheduled


class KeyYielder(scheduled.KeyYielder):

    def yield_key(self):
        for i in range(10):
            yield i


def test_dist():
    q = scheduled.RedisQueue('test:demo', {}) 
    yielder = KeyYielder()
    publisher = scheduled.Publisher(q, yielder)
    publisher.run()


class Fetcher(scheduled.Fetcher):

    def initialize(self):
        self.i = 0

    def fetch(self, key):
        print(key, self.i)
        self.i += 1
        return 'result:' + key, {}

    def after_fetch(self, key, data, metadata):
        print(key, data)


def test_publishe_worker():
    q = scheduled.RedisQueue('test:demo', {}) 
    yielder = KeyYielder()
    publisher = scheduled.Publisher(q, yielder)
    publisher.run()

    queue = scheduled.RedisQueue('test:demo', {}) 
    fetcher = Fetcher()

    worker = scheduled.Worker(queue, fetcher=fetcher, pipelines=[])
    err = worker.run()
    print('errno', err)


if 1:
    import logging
    logging.basicConfig(level=logging.DEBUG)
    test_publishe_worker()
