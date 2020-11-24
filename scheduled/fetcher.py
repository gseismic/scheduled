import abc
import six
from .logger import worker_log


class Fetcher(six.with_metaclass(abc.ABCMeta)):

    def __init__(self, config=None, logger=None): 
        self.config = config or {}
        self.logger = logger or worker_log
        self.initialize()

    def initialize(self):
        pass

    def before_fetch(self, key):
        pass

    @abc.abstractmethod
    def fetch(self, key):
        raise NotImplementedError()

    def after_fetch(self, key, data, metadata):
        pass

    def finalize(self):
        pass
