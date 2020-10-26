import time
import enum
import traceback
import logging
# from .logger import get_logger


_logger = logging.getLogger(__name__)

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
        self.logger = logger or _logger

        # self.run_mode = self.config.get('run_mode', 'once')
        self.recheck_count = self.config.get('recheck_count', 3)
        self.recheck_sleep = self.config.get('recheck_sleep', 1.0)
        self.worker_interval = self.config.get('worker_interval', 1.0)
        self.init_error_to_todo = self.config.get('init_error_to_todo', True)
        # self.n_retry_if_error = self.config.get('n_retry_if_error', 3)

        # state
        self._run_id = None
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

    def run(self):
        self.logger.info('Running ...')
        todo_keys, doing_keys, done_keys, error_keys, null_keys = self.get_keys_info()
        self.print_queue_info(todo_keys, doing_keys, done_keys, error_keys, null_keys)

        if error_keys and self.init_error_to_todo:
            self.logger.info('Move errror-key -> todo-key ...3 sec ...')
            time.sleep(3)
            self.queue.all_errors_to_todos()

        # n_done, n_todo, n_errors = 0, 0, 0
        i = 0
        retry_countdown = self.recheck_count
        while True:
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
                data, meta = self.fetcher.fetch(key)
                meta = meta or {}
                if data is not None:
                    for pipeline in self.pipelines:
                        data, meta = pipeline.process(key, data, meta)
                    errno, msg = 0, 'Success'
                    self.queue.doing_to_done(key, reverse=False)
                    self.logger.debug('Done: %s' % key)
                else:
                    errno, msg = 1, 'Null value fetched'
                    self.queue.doing_to_null(key, reverse=False)
                    self.logger.warning('Null: %d: %s' % (errno, msg))
                time.sleep(self.worker_interval)
                i += 1
            except (KeyboardInterrupt, SystemExit):
                errno, msg = -999, 'Key `%s` pushed back, Stopped by user.' % key
                self.queue.doing_to_todo(key, reverse=False)
                self.logger.info(msg)
                break
            except Exception as e:
                self.logger.error(traceback.format_exc())
                errno, msg = -1, 'Key `%s` pushed back, detail: %s' % (key, str(e))
                self.queue.doing_to_error(key, reverse=True)
                self.logger.error(msg)
                time.sleep(self.worker_interval)

        todo_keys, doing_keys, done_keys, error_keys, null_keys = self.get_keys_info()
        self.logger.info('Done keys: [%s ...]: num=%d' % (str(done_keys[:5]), len(done_keys)))
        self.logger.info('Doing keys: %s, num=%d' % (str(doing_keys), len(doing_keys)))
        self.logger.info('Error keys: %s, num=%d' % (str(error_keys), len(error_keys)))
        self.logger.info('TODO keys: [%s ...], num=%d' % (str(todo_keys[:5]), len(todo_keys)))
        self.logger.info('NULL keys: %s, num=%d' % (str(null_keys), len(null_keys)))

        # n_done_key = len(done_keys)
        n_error_key = len(error_keys)
        return n_error_key
