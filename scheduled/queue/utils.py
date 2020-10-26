import re


def parse_redis_uri(uri):
    # redis://:password@localhost:6379/0
    # redis://localhost:6379/0
    scheme = None
    password = None
    host = None
    port = None
    db = None

    parts = uri.strip().split('://')
    scheme = parts[0]
    info = parts[1]

    if '@' in info:
        _12 = info.split('@')
        assert(len(_12) == 2)
        h_password, host_port_db = _12
        assert(h_password.startswith(':'))
        password = h_password[1:]
    else:
        host_port_db = info

    o = re.match('(.*?):(\d+)/(\d+)', host_port_db)
    if o:
        host = o.group(1)
        port = o.group(2)
        db = o.group(3)

    return scheme, password, host, port, db


if __name__ == '__main__':
    uri = 'redis://:password@localhost:6379/0'
    print(uri)
    scheme, password, host, port, db = parse_redis_uri(uri)
    print(scheme, password, host, port, db)

    uri = 'redis://localhost:6379/0'
    print(uri)
    scheme, password, host, port, db = parse_redis_uri(uri)
    print(scheme, password, host, port, db)
