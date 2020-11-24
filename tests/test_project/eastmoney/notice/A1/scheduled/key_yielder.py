import abc
import six
from .utils import get_logger


class KeyYielder(six.with_metaclass(abc.ABCMeta)):

    def __init__(self, config=None, logger=None):
        self.config = config or {}
        self.logger = logger or get_logger('scheduled')

    @abc.abstractmethod
    def yield_key(self):
        raise NotImplementedError()
