import time
#import uuid
from .logger import get_logger


class Distributer(object):

    def __init__(self, queue, key_yielder, config=None, logger=None):
        self.queue = queue 
        self.key_yielder = key_yielder
        self.config = config or {}
        self.logger = logger or get_logger()

        # self.run_mode = self.config.get('run_mode', 'once')
        self.recheck_count = self.config.get('recheck_count', 3)
        self.recheck_sleep = self.config.get('recheck_sleep', 1.0)
        self.yield_interval = self.config.get('yield_interval', 0)
        self.init_reset = self.config.get('init_reset', True)
        # self._run_id = None

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
        # self._run_id = str(uuid.uuid4())
        # self.logger.info('Running [%s] ...' % self._run_id)
        self.logger.info('Running ...')
        todo_keys, doing_keys, done_keys, error_keys, null_keys = self.get_keys_info()
        self.print_queue_info(todo_keys, doing_keys, done_keys, error_keys, null_keys)

        if self.init_reset:
            # 如果不为空，应该保存
            self.logger.info('Reseting All Keys[todo/doing/done/error] ..sleep 10 sec')
            if todo_keys:
                self.logger.warning('`TODO` is not empty [len=%d]...' % len(todo_keys))
            time.sleep(10)
            self.queue.reset()

        n_inserted = 0
        errno, msg = 0, 'succ'
        countdown = self.recheck_count
        fn_yielder = self.key_yielder.yield_key()
        while True:
            try:
                key_available = True
                try:
                    key = next(fn_yielder)
                except StopIteration:
                    key_available = False

                # self.logger.debug('Generated key %s ...' % key)
                if key == None or not key_available:
                    if countdown > 0:
                        self.logger.info('Maybe all tasks finished, ' 
                                         'Rechecking (%d) ...' % countdown)
                        time.sleep(self.recheck_sleep)
                        countdown -= 1
                        continue
                    else:
                        self.logger.info('All tasks finished!')
                        break
                else:
                    countdown = self.recheck_count

                # run_key = '%s:%s' % (self._run_id, key)
                # self.queue.put_key(run_key)
                # self.logger.debug('Inserted: %d: key: %s' % (n_inserted + 1, run_key))
                self.queue.push_key(key)
                n_inserted += 1
                self.logger.info('Inserted: %d: key: %s' % (n_inserted, key))

                if self.yield_interval > 0:
                    time.sleep(self.yield_interval)
            except KeyboardInterrupt:
                errno = -1
                msg = 'Stopped by user, Re-Run required.'
                self.logger.info(msg)
                break
            except Exception as e:
                msg = 'Error: %s' % str(e)
                self.logger.info(msg)
                errno = -2
                break

        if errno == 0:
            self.logger.notice('Done: Total inserted: %d' % n_inserted)
        elif errno == -1:
            self.logger.warning(msg)
        else:
            self.logger.error('Half Done: Total inserted: %d' % n_inserted)

        return errno
