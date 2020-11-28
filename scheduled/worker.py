import time
import enum
import signal
import traceback
from .logger import worker_log
from .config import default_config


class WorkerState(enum.IntEnum):
    NotStarted = -1
    Success = 0
    Running = 1
    Retrying = 2
    Finished = 3


class Worker(object):
    """
    not periodic-mode
    """

    def __init__(self, queue, fetcher, pipelines, 
                 config=None, logger=None):
        self.queue = queue 
        self.fetcher = fetcher
        self.pipelines = pipelines
        self.config = config or {}
        self.logger = logger or worker_log

        # self.run_mode = self.config.get('run_mode', 'once')
        self.recheck_count = self.config.get('recheck_count', 3)
        self.recheck_sleep = self.config.get('recheck_sleep', 1.0)
        self.interval = self.config.get('interval', 1.0)
        self.init_error_to_todo = self.config.get('init_error_to_todo', True)
        # self.n_retry_if_error = self.config.get('n_retry_if_error', 3)

        # state
        self._run_id = None
        # 重置
        self.queue.mark_stop(0)
        self._should_stop = False # manually
        # self._state = WorkerState.NotStarted

    def get_keys_info(self):
        todo_keys = self.queue.get_todo_keys()
        doing_keys = self.queue.get_doing_keys()
        done_keys = self.queue.get_done_keys()
        error_keys = self.queue.get_error_keys()
        null_keys = self.queue.get_null_keys()
        return todo_keys, doing_keys, done_keys, error_keys, null_keys

    def print_queue_info(self, todo_keys, doing_keys, done_keys, error_keys, null_keys):
        self.logger.info('Done keys: %s ...: num=%d' % (str(done_keys[:5]), len(done_keys)))
        self.logger.info('Doing keys: %s, num=%d' % (str(doing_keys), len(doing_keys)))
        self.logger.info('Error keys: %s, num=%d' % (str(error_keys), len(error_keys)))
        self.logger.info('TODO keys: %s ..., num=%d' % (str(todo_keys[:5]), len(todo_keys)))
        self.logger.info('NULL keys: %s, num=%d' % (str(null_keys), len(null_keys)))

    def should_stop(self):
        rv = False
        if self.queue.test_should_stop() or self._should_stop:
            rv = True
        return rv

    def run(self):
        while 1:
            if self.should_stop():
                self.logger.info('Stop [should_stop==1]')
                break

            n_unfished_key, force_exit = self._run()
            if force_exit or n_unfished_key == 0:
                break
            time.sleep(10)

    def _run(self):
        self.logger.info('Running ...')
        todo_keys, doing_keys, done_keys, error_keys, null_keys = self.get_keys_info()
        self.print_queue_info(todo_keys, doing_keys, done_keys, error_keys, null_keys)

        if error_keys and self.init_error_to_todo:
            self.logger.info('Move errror-key -> todo-key ...3 sec ...')
            time.sleep(3)
            self.logger.info('`errors` to `todo` ..')
            self.queue.all_errors_to_todos()
            # self.logger.info('`doing` to `todo` ..')
            # self.queue.all_doings_to_todos()

        # n_done, n_todo, n_errors = 0, 0, 0
        force_exit = False
        i = 0
        retry_countdown = self.recheck_count
        prev_time = time.time()
        warm_stop_interval = default_config.get('warm_stop_interval', 1)
        while True:
            now = time.time()
            if now - prev_time > warm_stop_interval:
                if self.should_stop():
                    self.logger.info('Stop [`mark` reason]')
                    break
                prev_time = now

            key = self.queue.pop_key() # [todo] -> [doing]
            self.logger.info('%d: key: %s' % (i + 1, key))

            if key == None:
                """ 没有了，等。。。"""
                if retry_countdown > 0:
                    self.logger.info('No tasks available, '
                                     '(%d) checks to be continued...' % retry_countdown)
                    time.sleep(self.recheck_sleep)
                    retry_countdown -= 1
                    continue
                else:
                    self.logger.info('All tasks finished!')
                    break
            else:
                retry_countdown = self.recheck_count

            errno, msg = 0, None
            try:
                self.fetcher.before_fetch(key)
                data, metadata = self.fetcher.fetch(key)
                self.fetcher.after_fetch(key, data, metadata)
                metadata = metadata or {}
                metadata.update({'key': key})

                if data is not None:
                    for pipeline in self.pipelines:
                        pipeline.before_process(key, data, metadata)
                        data, metadata = pipeline.process(key, data, metadata)
                        pipeline.after_process(key, data, metadata)
                    errno, msg = 0, 'Success'
                    self.queue.doing_to_done(key, reverse=False)
                    self.logger.debug('Done: %s' % key)
                else:
                    errno, msg = 1, 'Null value fetched'
                    self.queue.doing_to_null(key, reverse=False)
                    self.logger.warning('Null: %d: %s' % (errno, msg))
                time.sleep(self.interval)
                i += 1
            except (KeyboardInterrupt, SystemExit):
                errno, msg = -999, 'Key `%s` pushed back, Stopped by user.' % key
                self.queue.doing_to_todo(key, reverse=False)
                self.logger.info(msg)
                force_exit = True
                break
            except Exception as e:
                self.logger.error(traceback.format_exc())
                errno, msg = -1, 'Key `%s` pushed back, detail: %s' % (key, str(e))
                self.queue.doing_to_error(key, reverse=True)
                self.logger.error(msg)
                time.sleep(self.interval)

        todo_keys, doing_keys, done_keys, error_keys, null_keys = self.get_keys_info()
        self.logger.info('Done keys: [%s ...]: num=%d' % (str(done_keys[:5]), len(done_keys)))
        self.logger.info('Doing keys: %s, num=%d' % (str(doing_keys), len(doing_keys)))
        self.logger.info('Error keys: %s, num=%d' % (str(error_keys), len(error_keys)))
        self.logger.info('TODO keys: [%s ...], num=%d' % (str(todo_keys[:5]), len(todo_keys)))
        self.logger.info('NULL keys: %s, num=%d' % (str(null_keys), len(null_keys)))

        n_unfished_key = len(doing_keys) + len(todo_keys)
        # print('not done: ', n_unfished_key)
        if n_unfished_key > 0:
            self.logger.info('【未完成】: [n_todo/🍉]=%d, [n_doing/🍊]=%d, [n_error/👀]=%d' % (
                len(todo_keys), len(doing_keys), len(error_keys)
            ))
        else:
            self.logger.info('【完成】: [n_done/🍅]=%d, [n_null🥔]=%d' % (
                len(done_keys), len(null_keys)
            ))
        return n_unfished_key, force_exit
