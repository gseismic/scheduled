import abc
import six
from .logger import get_logger


class Fetcher(six.with_metaclass(abc.ABCMeta)):

    def __init__(self, config=None, logger=None):
        self.config = config or {}
        self.logger = logger or get_logger()

    @abc.abstractmethod
    def fetch(self, key):
        raise NotImplementedError()
