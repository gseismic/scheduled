# coding: utf8
import six
import abc
from ..utils import get_logger


class BaseStorage(six.with_metaclass(abc.ABCMeta)):

    def __init__(self, config=None, logger=None):
        self.config = config or {}
        self.logger = logger or get_logger(name="main")

    @abc.abstractmethod
    def reset(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def put_key(self, key):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_key(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def doing_to_todo(self, key, end=True):
        raise NotImplementedError()

    @abc.abstractmethod
    def doing_to_done(self, key, end=True):
        raise NotImplementedError()

    @abc.abstractmethod
    def doing_to_error(self, key, end=True):
        raise NotImplementedError()
