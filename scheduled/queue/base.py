# coding: utf8
import six
import abc


class BaseQueue(six.with_metaclass(abc.ABCMeta)):

    @abc.abstractmethod
    def reset(self, todo=True, doing=True, done=True, error=True, null=True):
        raise NotImplementedError()

    @abc.abstractmethod
    def push_key(self, key):
        raise NotImplementedError()

    @abc.abstractmethod
    def pop_key(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def doing_to_todo(self, key, reverse=False):
        raise NotImplementedError()

    @abc.abstractmethod
    def doing_to_done(self, key, reverse=False):
        raise NotImplementedError()

    @abc.abstractmethod
    def doing_to_null(self, key, reverse=False):
        raise NotImplementedError()

    @abc.abstractmethod
    def doing_to_error(self, key, reverse=False):
        raise NotImplementedError()

    @abc.abstractmethod
    def all_errors_to_todos(self, reverse=False):
        raise NotImplementedError()

    @abc.abstractmethod
    def all_nulls_to_todos(self, reverse=False):
        # key 的结果为None: -> result
        raise NotImplementedError()
