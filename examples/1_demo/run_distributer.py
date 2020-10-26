import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')


import scheduled

class KeyYielder(scheduled.KeyYielder):
    def yield_key(self):
        for i in range(100):
            yield i


q = scheduled.RedisQueue('test:demo', {}) 
yielder = KeyYielder()
distributer = scheduled.Distributer(q, yielder)
distributer.run()
