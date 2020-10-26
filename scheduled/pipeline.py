import abc
import six
import logging


_logger = logging.getLogger(__name__)

class Pipeline(six.with_metaclass(abc.ABCMeta)):

    def __init__(self, config=None, logger=None):
        self.config = config or {}
        self.logger = logger or _logger

    @abc.abstractmethod
    def process(self, key, data, options):
        raise NotImplementedError()
