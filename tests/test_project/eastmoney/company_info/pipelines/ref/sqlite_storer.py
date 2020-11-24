# coding: utf8
import os
import arrow
import datetime
from scheduled import Pipeline
from finapi.common.collections import DefaultOrderedDict
from .const import table_fields
from .sqliteutil import Database, Table


def format_datetime(s):
    d = arrow.get(s)
    return d.format('YYYY-MM-DD HH:mm:ss')


class SqliteStorer(Pipeline):

    def __init__(self, *args, **kwargs):
        super(SqliteStorer, self).__init__(*args, **kwargs)
        if not os.path.exists(self.config['db_root_path']):
            os.makedirs(self.config['db_root_path'])

    def split_by_year(self, data):
        odata = DefaultOrderedDict(list)
        for idx, item in enumerate(data):
            year = item['EUTIME'].split('T')[0].split('-')[0]
            odata[year].append(item)
        return odata

    def process(self, key, data, options):
        if not data:
            return data, options

        # finance_date 中的year
        yearly_data = self.split_by_year(data)

        for year, items in yearly_data.items():
            if not items:
                continue
            # 
            unit = 1
            basename = 'eastmoney_finance_notice.%d.%dy.sqlite3' % (int(int(year)/unit)*unit, unit)
            db_file = os.path.join(self.config['db_root_path'], basename)
            db = Database(db_file)
            db.execute('PRAGMA synchronous = OFF')

            # table_name = 'year_%s' % str(year)
            table_name = 'notice'
            table = Table(db, table_name=table_name,
                          table_fields=table_fields)

            table.create_table(ignore_if_exists=True)
            updated_at = arrow.now().format('YYYY-MM-DD HH:mm:ss')
            o_items = []
            for idx, item in enumerate(items):
                # print(item)
                dic = {}
                dic['code'] = item['code']
                dic['name'] = item['name']
                publish_time = arrow.get(item['EUTIME'])
                dic['publish_time'] = publish_time.format('YYYY-MM-DD HH:mm:ss')
                _time = publish_time.time()
                if  _time > datetime.time(15, 0, 0):
                    dic['publish_time_type'] = 'Evening'
                elif _time < datetime.time(9, 10, 0):
                    dic['publish_time_type'] = 'Morning'
                elif _time >= datetime.time(11, 31, 1) and _time <= datetime.time(12, 59, 59):
                    dic['publish_time_type'] = 'Noon'
                else:
                    dic['publish_time_type'] = 'Market'
                dic['class_code'] = options['first_code']
                dic['class_name'] = options['first_name']
                dic['subclass_code'] = options['second_code']
                dic['subclass_name'] = options['second_name']
                dic['type_name'] = item['type_name']
                dic['type_code'] = item['type_code']
                dic['type_name'] = item['type_name']
                dic['title'] = item['title']
                dic['attach_type'] = item['attach_type']
                dic['attach_size'] = item['attach_size']
                dic['maybe_attach_url'] = item['maybe_attach_url']
                dic['detail_url'] = item['detail_url']
                dic['info_code'] = item['info_code']
                dic['listing_state'] = item['listing_state']
                dic['_notice_time'] = format_datetime(item['NOTICEDATE'])
                dic['_end_time'] = format_datetime(item['ENDDATE'])
                dic['extra'] = ''
                dic['reserved'] = ''
                dic['updated_at'] = updated_at
                # print(dic)
                o_items.append(dic)
                assert(len(dic) == len(table_fields))
            assert(len(o_items[0]) == len(table_fields))
            table.dict_insertmany(o_items, action='insert or ignore into',
                                  chunk_size=1000, commit=True)
            # create index
            table.create_index(ignore_if_exists=True)
        return data, options
