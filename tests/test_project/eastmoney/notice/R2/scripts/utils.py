import os
import types
import pickle
import functools


def file_cache(fun, *args, **kwargs):

    @functools.wraps(fun)
    def call_fun(*args, **kwargs):
        if not os.path.exists('.cache'):
            print('call ..')
            result = fun(*args, **kwargs)
            with open('.cache', 'wb') as f:
                f.write(pickle.dumps(result))
        else:
            print('cache ..')
            with open('.cache', 'rb') as f:
                text = f.read()
                print(type(text))
                print(text)
                if text:
                    result = pickle.loads(text)
                    print('result', result)
                    print(type(result))
                else:
                    result = None
        return result

    return call_fun


def filecache(filename=None):
    assert(type(filename) in [None, str])
    def _file_cache(fun):
        @functools.wraps(fun)
        def call_fun(*args, **kwargs):
            _id = '%s-%s' % (fun.__module__, fun.__name__)
            _filename = filename
            # print('filename', _filename)
            if _filename is None:
                _filename = 'cache.%s' % _id
            # print(call_fun.__name__) # tes
            if not os.path.exists(_filename):
                result = fun(*args, **kwargs)
                print(type(_filename))
                with open(_filename, 'wb') as f:
                    f.write(pickle.dumps(result))
            else:
                print('Warning: Calling from Cache ...')
                with open(_filename, 'rb') as f:
                    text = f.read()
                    if text:
                        result = pickle.loads(text)
                    else:
                        result = None
            return result
        return call_fun

    if type(filename) == types.FunctionType:
        # https://github.com/ubershmekel/filecache/blob/master/filecache/__init__.py
        # support for when people use '@filecache.filecache'
        # instead of '@filecache.filecache()'
        func = filename
        filename = None
        return _filecache(func)
    return _file_cache


if __name__ == '__main__':
    import time
    if 0:
        @file_cache
        def test():
            time.sleep(5)
            return '1asdfasdf'
        print(test())
    if 1:
        @filecache(filename='cache')
        def test(a, b):
            print(a, b)
            time.sleep(5)
            return a+b
        print(test(3, 5))
