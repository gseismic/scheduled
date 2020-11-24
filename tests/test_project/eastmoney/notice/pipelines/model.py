from peewee import *


# ref https://www.osgeo.cn/peewee/peewee/database.html
db = SqliteDatabase(db_path, pragmas={
    'journal_mode': 'wal',
    'cache_size': -1024 * 64})


class Notice(Model):

    name = TextField(index=True)
    # code = C

table_fields = [
    {'name': 'code', 'dtype': 'CHAR(10)', 'index': True, 'nullable': False, 'primary': True},
    {'name': 'name', 'dtype': 'text', 'nullable': False},
    {'name': 'publish_time', 'dtype': 'datetime', 'index': True, 'primary': True},
    {'name': 'publish_time_type', 'dtype': 'text', 'index': True},
    {'name': 'class_code', 'dtype': 'text', 'index': True},
    {'name': 'class_name', 'dtype': 'text', 'index': True},
    {'name': 'subclass_code', 'dtype': 'text', 'index': True},
    {'name': 'subclass_name', 'dtype': 'text', 'index': True},
    {'name': 'type_code', 'dtype': 'text', 'index': True},
    {'name': 'type_name', 'dtype': 'text', 'index': True},
    {'name': 'title', 'dtype': 'text'},
    {'name': 'attach_type', 'dtype': 'text', 'index': True},
    {'name': 'attach_size', 'dtype': 'text'},
    {'name': 'maybe_attach_url', 'dtype': 'text'},
    {'name': 'detail_url', 'dtype': 'text'},
    # not so important
    {'name': 'info_code', 'dtype': 'text', 'primary': True},
    {'name': 'listing_state', 'dtype': 'text'},
    {'name': '_notice_time', 'dtype': 'datetime', 'index': True},
    {'name': '_end_time', 'dtype': 'datetime', 'index': True},
    {'name': 'extra', 'dtype': 'text'}, # 额外信息
    {'name': 'reserved', 'dtype': 'text'}, # e.g. 是否需要清理标志
    {'name': 'updated_at', 'dtype': 'datetime', 'nullable': False, 'index': True},
]
