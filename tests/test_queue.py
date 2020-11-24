import time
import config
import random
from scheduled import RedisQueue


def test_push_pop():
    q = RedisQueue('test:demo', redis_uri={})
    time.sleep(random.random())
    q.reset()
    # assert(q.get_todo_keys() == [])

    q.push_key('key1')
    q.push_key('key2')

    print(q.get_todo_keys())
    assert(q.get_todo_keys() == ['key1', 'key2'])
    key1 = q.pop_key()
    key2 = q.pop_key()
    assert(key1 == 'key1')
    assert(key2 == 'key2')
    assert(q.get_todo_keys() == [])


def test_move_key():
    q = RedisQueue('test:demo', redis_uri={})
    time.sleep(random.random())
    q.reset()
    q.push_key('key1')
    q.push_key('key2')

    key1 = q.pop_key()
    assert(key1 == 'key1')
    assert(q.get_doing_keys() == set(['key1']))
    key2 = q.pop_key()
    assert(key2 == 'key2')
    assert(q.get_doing_keys() == set(['key1', 'key2']))

    q.doing_to_done('key1')
    assert(q.get_doing_keys() == set(['key2']))
    assert(q.get_done_keys() == ['key1'])

    q.doing_to_done('key2')
    assert(q.get_doing_keys() == set())
    assert(q.get_done_keys() == ['key1', 'key2'])
    # assert(q.get_done_keys() == ['key2', 'key1'])
    # q.doing_to_error('key2')


def test_done():
    q = RedisQueue('test:demo', redis_uri={})
    time.sleep(random.random())
    q.reset()
    q.push_key('key1')
    q.push_key('key2')

    key1 = q.pop_key()
    key2 = q.pop_key()

    assert q.get_doing_keys() == set(['key1', 'key2'])

    # to error
    assert q.doing_to_done('not_exist_key') is False
    assert q.doing_to_done('key1') is True
    assert q.doing_to_done('key2') is True
    assert q.get_done_keys() == ['key1', 'key2']


def test_error():
    q = RedisQueue('test:demo', redis_uri={})
    time.sleep(random.random())
    q.reset()
    q.push_key('key1')
    q.push_key('key2')

    key1 = q.pop_key()
    key2 = q.pop_key()

    assert q.get_doing_keys() == set(['key1', 'key2'])

    # to error
    assert q.doing_to_error('not_exist_key') is False
    assert q.doing_to_error('key1') is True
    assert q.doing_to_error('key2') is True
    assert q.get_error_keys() == ['key1', 'key2']


def test_doing_to_todo():
    q = RedisQueue('test:demo', redis_uri={})
    time.sleep(random.random())
    q.reset()
    q.push_key('key1')
    q.push_key('key2')
    key1 = q.pop_key()
    key2 = q.pop_key()

    assert q.get_doing_keys() == set(['key1', 'key2'])
    # to error
    assert q.doing_to_todo('not_exist_key') is False
    assert q.doing_to_todo('key1') is True
    assert q.doing_to_todo('key2') is True
    assert q.get_todo_keys() == ['key1', 'key2']


def test_doing_to_null():
    q = RedisQueue('test:demo', redis_uri={})
    time.sleep(random.random())
    q.reset()
    q.push_key('key1')
    q.push_key('key2')
    key1 = q.pop_key()
    key2 = q.pop_key()

    assert q.get_doing_keys() == set(['key1', 'key2'])
    # to error
    assert q.doing_to_null('not_exist_key') is False
    assert q.doing_to_null('key1') is True
    assert q.doing_to_null('key2') is True
    assert q.get_null_keys() == ['key1', 'key2']


def test_5():
    q = RedisQueue('test:demo', redis_uri={})
    time.sleep(random.random())
    q.reset()
    q.push_key('key1')
    q.push_key('key2')
    key1 = q.pop_key()
    key2 = q.pop_key()

    assert q.doing_to_null('key1') is True
    assert q.doing_to_null('key2') is True
    assert(q.get_doing_keys() == set())
    assert q.get_null_keys() == ['key1', 'key2']

    assert q.all_nulls_to_todos() == 2
    assert q.get_null_keys() == []
    assert q.get_todo_keys() == ['key1', 'key2']


def test_6():
    q = RedisQueue('test:demo', redis_uri={})
    time.sleep(random.random())
    q.reset()
    q.push_key('key1')
    q.push_key('key2')
    key1 = q.pop_key()
    key2 = q.pop_key()

    assert q.doing_to_error('key1') is True
    assert q.doing_to_error('key2') is True
    assert(q.get_doing_keys() == set())
    assert q.get_error_keys() == ['key1', 'key2']
    assert q.all_errors_to_todos() == 2
    assert q.get_error_keys() == []
    assert q.get_todo_keys() == ['key1', 'key2']


if 1:
    test_error()
