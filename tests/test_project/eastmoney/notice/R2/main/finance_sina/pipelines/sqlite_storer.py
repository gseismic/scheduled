# coding: utf8
import os
import arrow
from ...scheduled import Pipeline
from sqliteutil.v2 import Database, Table
from finapi.common.collections import DefaultOrderedDict
from .model import DAYPRICE_NETEASE_MODEL


class SqliteStorer(Pipeline):

    def __init__(self, *args, **kwargs):
        super(SqliteStorer, self).__init__(*args, **kwargs)
        if not os.path.exists(self.config['db_root_path']):
            os.makedirs(self.config['db_root_path'])
        self.start_date = self.config.get('start_date')
        self.end_date = self.config.get('end_date')
        self.db_tagname = self.config.get('db_tagname', 'dayprice_netease')

    def split_by_year(self, data):
        odata = DefaultOrderedDict(list)
        for idx, item in enumerate(data):
            if idx == 0:
                continue
            year = item[0].split('-')[0]
            odata[year].append(item)
        return odata

    def process(self, key, data, options):
        if not data:
            return data, options

        type_ = options['type']
        yearly_data = self.split_by_year(data)
        updated_at = arrow.now().format('YYYY-MM-DD HH:mm:ss')

        for year, items in yearly_data.items():
            if not items:
                continue
            basename = '%s.%dc.sqlite3' % (self.db_tagname, int(int(year)/5)*5)
            db_file = os.path.join(self.config['db_root_path'], basename)

            # 数据安全第一，因为出现不这样设置数据mal的情况(硬盘出问题)
            db = Database(db_file)
            db.execute('PRAGMA synchronous = NORMAL')
            db.execute('PRAGMA journal_mode = WAL')

            table_name = 'year_%d' % int(year)
            table = Table(db, table_name, DAYPRICE_NETEASE_MODEL)
            table.create_table(ignore_if_exists=True)

            extra = ''
            reserved = ''
            ext = [type_, extra, reserved, updated_at]
            list_items = []
            for idx, item in enumerate(items):
                _item = item
                _item.insert(2, item[1])
                _item[1] = options['symbol'].upper()
                _item.extend(ext)
                list_items.append(_item)

            action='insert or ignore into'
            table.list_insertmany(list_items, action=action, commit=True)
            # create index
            table.create_index(ignore_if_exists=True)
            table.close()
            db.close()
        return data, options
