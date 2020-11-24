import time
from .utils import get_logger


class Distributer(object):

    def __init__(self, storage, key_yielder, config=None, logger=None):
        self.storage = storage 
        self.key_yielder = key_yielder
        self.config = config or {}
        self.logger = logger or get_logger("scheduled")

        # self.run_mode = self.config.get('run_mode', 'once')
        self.recheck_count = self.config.get('recheck_count', 3)
        self.recheck_sleep = self.config.get('recheck_sleep', 1.0)
        self.generator_interval = self.config.get('generator_interval', 1.0)
        self.init_reset = self.config.get('init_reset', True)

    def run(self):
        # status, msg = -999, 'Not Started'

        chkcnt = self.recheck_count
        # n_done, n_todo, n_errors = 0, 0, 0
        self.logger.info('Running ...')

        if self.init_reset:
            self.logger.info('Reseting ...')
            self.storage.reset()

        cnt = 0
        while True:
            key = self.key_yielder.yield_key()
            # self.logger.debug('Generated key %s ...' % key)

            if key == None:
                if chkcnt > 0:
                    self.logger.info('Maybe all tasks finished, ' \
                                     'Rechecking (%d) ...' % chkcnt)
                    time.sleep(self.recheck_sleep)
                    chkcnt -= 1
                    continue
                else:
                    self.logger.info('All tasks finished!')
                    break
            else:
                chkcnt = self.recheck_count

            self.storage.put_key(key)
            self.logger.debug('Inserted: %d: key: %s' % (cnt + 1, key))

            if self.generator_interval >= 0:
                time.sleep(self.generator_interval)

            cnt += 1

        self.logger.notice('Done: Total inserted: %d' % cnt)
