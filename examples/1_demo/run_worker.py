import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')

import scheduled


class KeyYielder(scheduled.KeyYielder):

    def yield_key(self):
        for i in range(100):
            yield i

class Fetcher(scheduled.Fetcher):

    def fetch(self, key):
        return 'result:' + key, {}

queue = scheduled.RedisQueue('test:demo', {})
fetcher = Fetcher()
worker = scheduled.Worker(queue, fetcher=fetcher, pipelines=[])
err = worker.run()
print('errno', err)
