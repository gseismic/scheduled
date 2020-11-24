import abc
import six
from .logger import publisher_log


class KeyYielder(six.with_metaclass(abc.ABCMeta)):

    def __init__(self, config=None, logger=None): 
        self.config = config or {}
        self.logger = logger or publisher_log
        self.initialize()

    def initialize(self):
        pass

    @abc.abstractmethod
    def yield_key(self):
        raise NotImplementedError()

    def finalize(self):
        pass
