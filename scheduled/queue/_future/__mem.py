# coding: utf8
from .base import BaseStorage


class MemStorage(BaseStorage):
    """
    可能被多个线程读写
    """

    def __init__(self, mutex):
        self._mutex = mutex
        self.reset()

    def reset(self):
        with self._mutex:
            self._todos = []
            self._dones = []
            self._errors = []
            self._doings = set()

    def put_key(self, key):
        with self._mutex:
            self._todos.append(key)

    def get_key(self):
        with self._mutex:
            key = self._todos.pop(0)
            self.self._doings.add(key)

    def doing_to_todo(self, key, end=True):
        with self._mutex:
            assert(key in self._doings)
            self._doings.remove(key)
            if end:
                self._todos.append(key)
            else:
                self._todos.insert(0, key)

    def doing_to_done(self, key, end=True):
        with self._mutex:
            assert(key in self._doings)
            self._doings.remove(key)
            if end:
                self._dones.append(key)
            else:
                self._dones.insert(0, key)

    def doing_to_error(self, key, end=True):
        with self._mutex:
            assert(key in self._doings)
            self._doings.remove(key)
            if end:
                self._errors.append(key)
            else:
                self._errors.insert(0, key)
