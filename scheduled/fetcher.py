import abc
import six
import logging


_logger = logging.getLogger(__name__)

class Fetcher(six.with_metaclass(abc.ABCMeta)):

    def __init__(self, config=None, logger=None): 
        self.config = config or {}
        self.logger = logger or _logger

    @abc.abstractmethod
    def fetch(self, key):
        raise NotImplementedError()
