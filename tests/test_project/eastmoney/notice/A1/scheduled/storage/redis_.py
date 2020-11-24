# coding: utf8
import redis
from .base import BaseStorage


class RedisStorage(BaseStorage):

    def __init__(self, config=None, logger=None):
        super(RedisStorage, self).__init__(config, logger)
        redis_key_prefix = self.config.get('redis_prefix', 'redis_prefix')
        redis_connection = self.config['redis_connection']
        self._todo_rkey = redis_key_prefix + ':todo'
        self._doing_rkey = redis_key_prefix + ':doing'
        self._done_rkey = redis_key_prefix + ':done'
        self._error_rkey = redis_key_prefix + ':error'
        self._null_rkey = redis_key_prefix + ':null'
        self.redis = redis.Redis(**redis_connection, decode_responses=True)

    def reset(self):
        # self.redis.clear(self._todo_rkey)
        self.redis.ltrim(self._todo_rkey, -1, 0)
        self.redis.delete(self._doing_rkey)
        self.redis.delete(self._done_rkey)
        self.redis.delete(self._error_rkey)
        self.redis.delete(self._null_rkey)

    def put_key(self, key):
        self.redis.lpush(self._todo_rkey, key)

    def get_key(self):
        key = self.redis.rpop(self._todo_rkey)
        # 2020-03-13 17:03:48
        if key is not None:
            self.redis.sadd(self._doing_rkey, key)
        return key

    def get_doing_keys(self):
        keys = self.redis.smembers(self._doing_rkey)
        return keys

    def get_todo_keys(self):
        keys = self.redis.lrange(self._todo_rkey, 0, -1)
        return keys

    def get_error_keys(self):
        keys = self.redis.lrange(self._error_rkey, 0, -1)
        return keys

    def get_done_keys(self):
        keys = self.redis.lrange(self._done_rkey, 0, -1)
        return keys

    def get_null_keys(self):
        keys = self.redis.lrange(self._null_rkey, 0, -1)
        return keys

    def doing_to_todo(self, key, reverse=False):
        self.redis.srem(self._doing_rkey, key)
        if reverse:
            self.redis.lpush(self._todo_rkey, key)
        else:
            self.redis.rpush(self._todo_rkey, key)

    def doing_to_null(self, key, reverse=False):
        self.redis.srem(self._doing_rkey, key)
        if reverse:
            self.redis.lpush(self._null_rkey, key)
        else:
            self.redis.rpush(self._null_rkey, key)

    def doing_to_done(self, key, reverse=False):
        self.redis.srem(self._doing_rkey, key)
        if reverse:
            self.redis.lpush(self._done_rkey, key)
        else:
            self.redis.rpush(self._done_rkey, key)

    def doing_to_error(self, key, reverse=False):
        self.redis.srem(self._doing_rkey, key)
        if reverse:
            self.redis.lpush(self._error_rkey, key)
        else:
            self.redis.lpop(self._error_rkey, key)

    def errors_to_todos(self, reverse=False):
        # reverse 代表放入 无论哪种情况，
        while True:
            key = self.redis.rpop(self._error_rkey)
            if key is None:
                break
            if not reverse:
                self.redis.lpush(self._todo_rkey, key)
            else:
                self.redis.rpush(self._todo_rkey, key)

    def nulls_to_todos(self, reverse=False):
        # reverse 代表放入 无论哪种情况，
        while True:
            key = self.redis.rpop(self._null_rkey)
            if key is None:
                break
            if not reverse:
                self.redis.lpush(self._todo_rkey, key)
            else:
                self.redis.rpush(self._todo_rkey, key)


if __name__ == '__main__':
    s = RedisStorage('test')
    if 0:
        s.put_key('key1')
        s.put_key('key2')
        key1 = s.get_key()
        key2 = s.get_key()
        print(key1)
        print(key2)
        assert(key1 == 'key1')
        assert(key2 == 'key2')
    if 1:
        s.put_key('k1')
        s.put_key('k2')
        s.doing_to_done('k1')
        s.doing_to_error('k2')
