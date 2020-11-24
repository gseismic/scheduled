import abc
import six
from .logger import worker_log


class Pipeline(six.with_metaclass(abc.ABCMeta)):

    def __init__(self, config=None, logger=None):
        self.config = config or {}
        self.logger = logger or worker_log
        self.initialize()

    def initialize(self):
        pass

    def before_process(self, key, data, metadata):
        pass

    @abc.abstractmethod
    def process(self, key, data, metadata):
        raise NotImplementedError()

    def after_process(self, key, data, metadata):
        pass

    def finalize(self):
        pass
