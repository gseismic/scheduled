from redis import Redis
from rqspider.queue import RedisQueue
from rqspider import Fetcher, Worker, TaskManager


class RedisQueue:

    def __init__(self, name, connection):
        pass


class Scheduler:

    def __init__(self, queue):
        pass

    def get_task(self):
        key = self.queue.get()


class Pipeline:

    def __init__(self, name):
        self.init()

    def init(self):
        pass

    def check_input(self, key, data, meta):
        pass

    def check_output(self, key, data, meta):
        pass

    def process(self, key, data, meta):
        pass

    def call(self, key, data, meta):
        self.check_input(key, data, meta)
        self.process(key, data, meta)
        self.check_output(key, data, meta)
        return data, meta


class Worker:

    def __init__(self, queue, fetcher, pipelines):
        self.queue = queue

    def task_done(self, key):
        pass

    def run(self):
        while 1:
            # 获得任务
            key = self.queue.get()
            if key == None:
                break

            try:
                # 获得数据
                data, meta = self.fetcher.fetch(key)
                # 处理数据
                for pipe in self.pipelines:
                    data, meta = pipe.process(key, data, meta)
            except Exception:
                ok = False


class Flow:

    def call(self, key, data, meta):
        pass


q = RedisQueue(name='demo', connection=r)

pipes = []

# (start) -> fetch -> pipe1 -> pipe2 -> (done)
worker = Worker(queue=q, fetcher=fetcher, pipelines=pipes)
worker.run()
'''
job0 ->
job1 -> \  |--------------|      / -> worker1
job2 ->  ->    Queue1       ->     -> worker2
job3 -> /  |--------------|      \ -> worker3
job7 ->

job6 ->
job8 -> \  |--------------|      / -> worker0
job9 ->  ->    Queue2       ->     -> worker2
job5 -> /  |--------------|      \ -> worker6
job10->

rqspider schedule job1 --cron
rqspider work --num-procs=5 high low default

rqspider scheduler   # 生成任务
rqspider worker

eastmoney.news.json
rqspider publish eastmoney.news
rqspider publish eastmoney.basic_info

rqspider work --num-procs=5 high low default
'''
