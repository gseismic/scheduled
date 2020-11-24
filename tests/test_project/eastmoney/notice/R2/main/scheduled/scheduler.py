import time
import traceback
from .utils import get_logger


class Scheduler(object):
    """
    not periodic-mode
    """

    def __init__(self, storage, fetcher, pipelines, 
                 config=None, logger=None, debug=False):
        self.storage = storage 
        self.fetcher = fetcher
        self.pipelines = pipelines
        self.config = config or {}
        self.logger = logger or get_logger()
        self.debug = debug

        # self.run_mode = self.config.get('run_mode', 'once')
        self.recheck_count = self.config.get('recheck_count', 3)
        self.recheck_sleep = self.config.get('recheck_sleep', 1.0)
        # 可以直接查看redis相关数据
        # self.redis_result_key = self.config.get('redis_result_key', 
        #                                         'schedule:redis_result_key')
        # self.n_retry_if_error = self.config.get('n_retry_if_error', 3)

    def run(self):
        self.logger.info('Running ...')

        done_keys = self.storage.get_done_keys()
        self.logger.notice('Done keys: [%s ...]: num=%d' % (str(done_keys[:5]), len(done_keys)))

        doing_keys = self.storage.get_doing_keys()
        self.logger.notice('Doing keys: %s, num=%d' % (str(doing_keys), len(doing_keys)))

        error_keys = self.storage.get_error_keys()
        self.logger.notice('Error keys: %s, num=%d' % (str(error_keys), len(error_keys)))

        todo_keys = self.storage.get_todo_keys()
        self.logger.notice('TODO keys: [%s ...], num=%d' % (str(todo_keys[:5]), len(todo_keys)))

        null_keys = self.storage.get_null_keys()
        self.logger.notice('NULL keys: %s, num=%d' % (str(null_keys), len(null_keys)))

        if self.config.get('init_error_to_todo', True):
            self.logger.notice('Move errror-key -> todo-key ...3 sec ...')
            time.sleep(3)
            self.storage.errors_to_todos()

        chkcnt = self.recheck_count
        # n_done, n_todo, n_errors = 0, 0, 0
        i = 0
        while True:
            try:
                key = self.storage.get_key() # [todo] -> [processing]
                self.logger.info('Gen: %d: key: %s' % (i + 1, key))

                if key == None:
                    """ 没有了，等。。。"""
                    if chkcnt > 0:
                        self.logger.debug('No tasks available, ' \
                                         '(%d) checks to be continued...' % chkcnt)
                        time.sleep(self.config['recheck_sleep'])
                        chkcnt -= 1
                        continue
                    else:
                        self.logger.info('All tasks finished!')
                        break
                else:
                    chkcnt = self.recheck_count

                errno, msg = 0, None
                # succ_fetch = False
                t0 = time.time()
                data, options = self.fetcher.fetch(key)
                t1 = time.time()
                if self.debug:
                    print('\t\tfetch time: %f' % (t1 - t0))
                # succ_fetch = True
                options = options or {}
                if data is not None:
                    for pipeline in self.pipelines:
                        data, options = pipeline.process(key, data, options)
                    errno, msg = 0, 'success'
                    self.storage.doing_to_done(key, reverse=False)
                else:
                    errno, msg = 1, 'Null value fetched'
                    # self.logger.notice('`%s`: None value, put to [null] ...' % key)
                    self.storage.doing_to_null(key, reverse=False)
                t2 = time.time()
                if self.debug:
                    print('\t\tprocess time: %f' % (t2 - t1))
                self.logger.debug('Done: %d: key: %s' % (i + 1, key))
            except KeyboardInterrupt:
                errno, msg = -999, 'Key `%s` pushed back, Stopped by user.' % key
                self.storage.doing_to_todo(key, reverse=False)
            except Exception as e:
                self.logger.error(traceback.format_exc())
                errno, msg = -1, 'Key `%s` pushed back, detail: %s' % (key, str(e))
                self.storage.doing_to_error(key, reverse=True)

            if errno == -999:
                self.logger.info(msg)
                break
            elif errno == 1:
                self.logger.warning('code: %d: %s' % (errno, msg))
            elif errno != 0:
                self.logger.error('Error: code: %d: %s' % (errno, msg))
            else:
                i += 1
                # self.logger.info('Success processing key: %s' % key)
            time.sleep(self.config['interval'])

        done_keys = self.storage.get_done_keys()
        self.logger.notice('Done keys: [%s ...]: num=%d' % (str(done_keys[:5]), len(done_keys)))

        doing_keys = self.storage.get_doing_keys()
        self.logger.notice('Doing keys: %s, num=%d' % (str(doing_keys), len(doing_keys)))

        error_keys = self.storage.get_error_keys()
        self.logger.notice('Error keys: %s, num=%d' % (str(error_keys), len(error_keys)))

        todo_keys = self.storage.get_todo_keys()
        self.logger.notice('TODO keys: [%s ...], num=%d' % (str(todo_keys[:5]), len(todo_keys)))

        null_keys = self.storage.get_null_keys()
        self.logger.notice('NULL keys: %s, num=%d' % (str(null_keys), len(null_keys)))

        # self.storage

        return len(error_keys)
