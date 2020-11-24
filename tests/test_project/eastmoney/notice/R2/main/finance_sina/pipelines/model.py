# encoding: utf8


# 按日期存储目标是：方便回测
# ['日期', '股票代码', '名称', '收盘价', '最高价', '最低价', '开盘价', '前收盘', '涨跌额', '涨跌幅', '换手率', '成交量', '成交金额', '总市值', '流通市值']
DAYPRICE_NETEASE_MODEL = [
    {'name': 'date', 'dtype': 'CHAR(10)', 'index': True, 'nullable': False, 'primary': True},
    {'name': 'symbol', 'dtype': 'CHAR(10)', 'index': True, 'nullable': False, 'primary': True},
    {'name': 'code', 'dtype': 'CHAR(8)', 'index': True, 'nullable': False},
    {'name': 'name', 'dtype': 'text', 'nullable': False},
    {'name': 'close', 'dtype': 'real'},
    {'name': 'high', 'dtype': 'real'},
    {'name': 'low', 'dtype': 'real'},
    {'name': 'open', 'dtype': 'real'},
    {'name': 'lclose', 'dtype': 'real'},
    {'name': 'chg', 'dtype': 'real'},
    {'name': 'pchg', 'dtype': 'real'},
    {'name': 'turnover_rate', 'dtype': 'real'},
    {'name': 'volume', 'dtype': 'real'},
    {'name': 'value', 'dtype': 'real'},
    {'name': 'mcap', 'dtype': 'real'},
    {'name': 'free_mcap', 'dtype': 'real'},
    {'name': 'type', 'dtype': 'CHAR', 'nullable': False, 'index': True},
    {'name': 'extra', 'dtype': 'text'},
    {'name': 'reserved', 'dtype': 'text'},
    {'name': 'updated_at', 'dtype': 'datetime', 'nullable': False, 'index': True},
]
