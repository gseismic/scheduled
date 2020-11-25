# coding: utf8
import redis
from .base import BaseQueue
from .utils import parse_redis_uri


class RedisQueue(BaseQueue):
    '''
    左入右出, 与list.pop 行为一致

    要求key是唯一的, doing 是set
    有可能有多个todo中的一样的key被添加入doing

    可能第一次todo中的key成功，第二次失败-> error，然后重试
    安全起见，key仍被作为error key处理

    这个比简单使用: lpush, rpop 好

    使用Redis好处:
        分布式布局方便
        可以本地运行，也可以分布式运行[实际处理结果还需要汇总到数据中心]

    每个key唯一
    没必要用mysql/sqlite3/pewee/pony.orm改写，因为不是调度
    要求key是唯一的
    todo -> doing- -> done
               \ - -> error 
                \- -> null  result_of(key) 无效[不需重试]

    运行结束: doing 非None, doing 可 -> `todo` 然后重试

    此处还可以记录 每个error-key的重试情况，每个key最多可重试多少次(redis汇总后的次数)
    '''
    def __init__(self, queue_id, redis_uri):
        '''
        # redis://user:password@localhost:port/0
        # redis://localhost:port/0
        '''
        self._todo_rkey = queue_id + ':todo'
        self._doing_rkey = queue_id + ':doing'
        self._done_rkey = queue_id + ':done'
        self._error_rkey = queue_id + ':error'
        self._null_rkey = queue_id + ':null'
        self._shouldstop_rkey = queue_id + ':shouldstop'
        redis_uri = redis_uri or {}
        if isinstance(redis_uri, str):
            scheme, password, host, port, db = parse_redis_uri(redis_uri)
        else:
            config = redis_uri
            host=config.get('host', 'localhost')
            port=config.get('port', 6379)
            password=config.get('password', None)
            db=config.get('db', 0)
        pool = redis.ConnectionPool(
            host=host, port=port, password=password, 
            db=db, decode_responses=True
        )
        self.redis = redis.Redis(connection_pool=pool)
        self.redis.set(self._shouldstop_rkey, 0)

    def reset(self, todo=True, doing=True, done=True, error=True, null=True):
        if todo:
            # self.redis.ltrim(self._todo_rkey, -1, 0)
            self.redis.delete(self._todo_rkey)
        if doing:
            self.redis.delete(self._doing_rkey)
        if done:
            self.redis.delete(self._done_rkey)
        if error:
            self.redis.delete(self._error_rkey)
        if null:
            self.redis.delete(self._null_rkey)

    def mark_stop(self, mark=1):
        self.redis.set(self._shouldstop_rkey, mark)

    def test_should_stop(self):
        r = self.redis.get(self._shouldstop_rkey)
        # print(repr(r))
        return int(r) == 1

    def push_key(self, key):
        # 与 a = [1, 2, 3]
        # a.pop() 结果顺序一致
        if key is not None:
            self.redis.lpush(self._todo_rkey, key)

    def pop_key(self):
        key = self.redis.rpop(self._todo_rkey)
        # 2020-03-13 17:03:48
        if key is not None:
            self.redis.sadd(self._doing_rkey, key)
        return key

    #def put_key(self, key):
    #    # 兼容
    #    self.push_key(key)

    #def get_key(self):
    #    # 向前兼容
    #    return self.pop_key()
    def doing_to_todo(self, key, reverse=False):
        return self._doing_to_xxx(key, self._todo_rkey, reverse)

    def doing_to_null(self, key, reverse=False):
        return self._doing_to_xxx(key, self._null_rkey, reverse)

    def doing_to_done(self, key, reverse=False):
        return self._doing_to_xxx(key, self._done_rkey, reverse)

    def doing_to_error(self, key, reverse=False):
        return self._doing_to_xxx(key, self._error_rkey, reverse)

    def _doing_to_xxx(self, key, redis_key, reverse=False):
        # Allow `doing`-key already deleted
        if reverse:
            self.redis.rpush(redis_key, key)
        else:
            self.redis.lpush(redis_key, key)
        r = self.redis.srem(self._doing_rkey, key)
        if r != 1:  
            return False
        return True

    def all_errors_to_todos(self, reverse=False):
        return self._all_xxx_to_todos(self._error_rkey, reverse)

    def all_nulls_to_todos(self, reverse=False):
        return self._all_xxx_to_todos(self._null_rkey, reverse)

    def all_doings_to_todos(self, reverse=False):
        # reverse 代表放入 无论哪种情况，
        n_keys = 0
        keys = self.redis.smembers(self._doing_rkey)
        pipe = self.redis.pipeline()
        for key in keys:
            if not reverse:
                pipe.lpush(self._todo_rkey, key)
            else:
                pipe.rpush(self._todo_rkey, key)
            n_keys += 1
        pipe.execute()
        # add first, del then
        # 可能执行时，别的进程也在执行
        n_del = 0
        for key in keys:
            n_del += pipe.srem(self._doing_rkey, key)
        r = pipe.execute()
        n_del = sum(r)
        return len(keys)

    def _all_xxx_to_todos(self, xxx_key, reverse=False):
        # reverse 代表放入 无论哪种情况，
        n_keys = 0
        # pipe = self.redis.pipeline()
        while True:
            key = self.redis.rpop(xxx_key)
            if key is None:
                break
            if not reverse:
                self.redis.lpush(self._todo_rkey, key)
            else:
                self.redis.rpush(self._todo_rkey, key)
            n_keys += 1
        return n_keys

    def get_doing_keys(self):
        keys = self.redis.smembers(self._doing_rkey)
        return keys

    def get_todo_keys(self):
        return self._get_xxx_keys(self._todo_rkey)

    def get_error_keys(self):
        return self._get_xxx_keys(self._error_rkey)

    def get_done_keys(self):
        return self._get_xxx_keys(self._done_rkey)

    def get_null_keys(self):
        return self._get_xxx_keys(self._null_rkey)

    def _get_xxx_keys(self, xxx_key):
        keys = self.redis.lrange(xxx_key, 0, -1)
        keys.reverse()
        return keys
