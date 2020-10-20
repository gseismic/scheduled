import scheduled

class KeyYielder(scheduled.KeyYielder):
    def yield_key(self):
        for i in range(100):
            yield i


q = scheduled.RedisQueue('test:demo', config={})
yielder = KeyYielder()
distributer = scheduled.Distributer(q, yielder)
distributer.run()
