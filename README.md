# scheduled
Demo Redis-based Task scheduler

## Structure
    fetcher     : fetch data from the Internet
    key_yielder : Key yielder
    distributer : Key Publisher
    worker      : Worker

## Demo
Task Publisher:
```python
import scheduled

class KeyYielder(scheduled.KeyYielder):
    def yield_key(self):
        for i in range(100):
            yield i


q = scheduled.RedisQueue('test:demo', config={})
yielder = KeyYielder()
distributer = scheduled.Publisher(q, yielder)
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

## ChangeLog
    * [@2020-10-26 18:35:12] logbook -> logging
    * [@2020-10-27 11:53:46] added: scripts
    * [@2020-11-25 03:09:58] v0.2.0 distributer --> publisher, suport multi-worker

## KnownIssue
    中途被强制中断：todo -> doing, 导致doing始终不为空，且不运行,
    要求不能出现强制中断的情况
