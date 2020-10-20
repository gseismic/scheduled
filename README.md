# scheduled
Demo Redis-based Task scheduler

## Structure
    fetcher     : fetch data from the Internet
    key_yielder : 生产Key
    distributer : 生产Key
    worker      : 运行
## Demo
Task Distributer:
```python
import scheduled

class KeyYielder(scheduled.KeyYielder):
    def yield_key(self):
        for i in range(100):
            yield i


q = scheduled.RedisQueue('test:demo', config={})
yielder = KeyYielder()
distributer = scheduled.Distributer(q, yielder)
distributer.run()
```

Worker:
```python
import scheduled

class KeyYielder(scheduled.KeyYielder):

    def yield_key(self):
        for i in range(100):
            yield i

class Fetcher(scheduled.Fetcher):

    def fetch(self, key):
        return 'result:' + key, {}

queue = scheduled.RedisQueue('test:demo', config={})
fetcher = Fetcher()
worker = scheduled.Worker(queue, fetcher=fetcher, pipelines=[])
err = worker.run()
print('errno', err)
```
