# coding: utf8
import os
import sqlite3
import traceback
from .pipeline import Pipeline
from finapi.common.collections import DefaultOrderedDict


class SqliteStorer(Pipeline):

    def __init__(self, *args, **kwargs):
        super(SqliteStorer, self).__init__(*args, **kwargs)
        if not os.path.exists(self.config['db_root_path']):
            os.makedirs(self.config['db_root_path'])
        self.start_date = self.config.get('start_date')
        self.end_date = self.config.get('end_date')

    def try_create_db(self, conn, cur, table_name):
        create_sql = 'CREATE TABLE if not exists %s (' \
                'symbol CHAR(10) NOT NULL, name TEXT,' \
                'date DATE NOT NULL,' \
                'open REAL, high REAL,' \
                'low REAL, close REAL,' \
                'volume REAL,' \
                'off_volume REAL,' \
                'off_value REAL,' \
                'type CHAR,' \
                'PRIMARY KEY(symbol, date))'
        cur.execute(create_sql % table_name)
        conn.commit()

    def try_create_index(self, conn, cur, table_name, field):
        sql = 'CREATE INDEX IF NOT EXISTS %s_index on %s(%s)' 
        cur.execute(sql % (field, table_name, field))
        conn.commit()

    def split_by_year(self, data):
        odata = DefaultOrderedDict(list)
        for item in data:
            year = item['d'].split('-')[0]
            odata[year].append(item)
        return odata

    def process_year_data(self, type_, symbol, year, ydata):
        # 2010 -> 2010s
        # 1970s, 2000s, 2010s, 2020s
        if self.start_date is not None:
            start_year = int(self.start_date[:4])
            if int(year) < start_year:
                return
        if self.end_date is not None:
            end_year = int(self.end_date[:4])
            if int(year) > end_year:
                return

        basename = 'sina_kline_day.%ds.sqlite3' % (int(int(year)/10)*10)
        db_file = os.path.join(self.config['db_root_path'],
                               basename)
        conn = sqlite3.connect(db_file)
        cur = conn.cursor()

        # create db
        table_name = 'year%d' % int(year)
        self.try_create_db(conn, cur, table_name)
        # slow down insertion speed
        self.try_create_index(conn, cur, table_name, field='symbol')
        self.try_create_index(conn, cur, table_name, field='date')
        self.try_create_index(conn, cur, table_name, field='type')

        many_values = []
        for item in ydata:
            # print(item['pv'])
            # assert(item['pv'] is None)
            if self.start_date is not None:
                if item['d'] < self.start_date:
                    # print(item, 'skip')
                    continue
            if self.end_date is not None:
                if item['d'] > self.end_date:
                    # print(item, 'skip')
                    continue
            value = [symbol,  '', item['d'], 
                     item['o'], item['h'], item['l'], 
                     item['c'], item['v'], 
                     item['pv'], item['pa'],
                     type_
                    ]
            many_values.append(value)

        if not many_values:
            return

        n_fields = len(many_values[0])
        action = 'insert or ignore into'
        _sql = '%s %s values' % (action, table_name)
        _args = '(' + ','.join(['?']*n_fields) + ')'
        sql = _sql + _args

        try:
            cur.executemany(sql, many_values)
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(traceback.format_exc())
            # self.logger.error('Exception: %s' % str(e))
            self.logger.debug('Failed to exec `%s`, rollback' % sql)
            # close
            cur.close()
            conn.close()
            raise e
        cur.close()
        conn.close()

    def process(self, data, key):
        symbol = key.split(':')[1].upper()
        type_ = key.split(':')[0].upper()
        years_data = self.split_by_year(data)
        for year, ydata in years_data.items():
            self.process_year_data(type_, symbol, year, ydata)
        return data
