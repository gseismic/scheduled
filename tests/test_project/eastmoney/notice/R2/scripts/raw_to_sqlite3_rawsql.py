import os
import glob
import pandas as pd
from collections import OrderedDict
#from finapi.common.filecache import filecache_static
from report_utils import process_field, zh_to_shortpy, get_sector_type, normalize_sheet


import sqlite3


pd.set_option('display.max_rows', None)


def process_kind_symbol(kind, sector, symbol, df, db_path):
    df, columns_zh = normalize_sheet(df)
    if df is None:
        return

    os.makedirs(db_path, exist_ok=True)
    # print(kind, sector, symbol)
    dbfile = os.path.join(db_path, 'finance_sina.%s.sqlite3' % (sector))

    conn = sqlite3.connect(dbfile)
    conn.execute('pragma journal_mode=wal;')

    tbl_name = kind
    sql_create = 'create table if not exists %s' % tbl_name
    sql_create += '(symbol text, findate text, findate_type text, '
    sql_create += ', '.join(['%s real' % f for f in df.columns])
    sql_create += ', primary key(symbol, findate))'

    try:
        cur = conn.cursor()
        cur.execute(sql_create)

        many_dict_values = []
        for index, row in df.iterrows():
            dic = OrderedDict()
            dic['findate'] = index
            dic['findate_type'] = index[4:]
            dic['symbol'] = symbol.upper()
            for k, val in row.items():
                dic[k] = val
            many_dict_values.append(dic)

        if many_dict_values:
            n_fields = len(many_dict_values[0])
            field_str = ','.join(list(many_dict_values[0].keys()))
            sql_insert = 'insert or ignore into %s(%s) values(%s)' % (
                tbl_name, field_str, ','.join(['?']*n_fields))
            cur.executemany(sql_insert, [tuple(item.values()) 
                                         for item in many_dict_values])
    finally:
        conn.commit()
        conn.close()


def process_kind(kind, root_path, db_path):
    datadir = root_path.rstrip('/') + '/' + kind + '/*.csv'
    filenames = glob.glob(datadir)
    for i, fn in enumerate(filenames):
        print(i, fn)
        symbol = fn.split('/')[-1].split('.')[0]

        df = pd.read_csv(fn)
        fields = tuple([process_field(f) for f in df['报表日期']])
        # shortpy_list = [zh_to_shortpy(f) for f in fields]
        sector = get_sector_type(kind, fields)
        # print(symbol, sector)
        process_kind_symbol(kind, sector, symbol, df, db_path)


def run():
    # 一切尊重原始数据，保持一一映射
    root_path = '/media/lsl/untitled/findata/astock/finance/sina/raw'
    # data_dir = './data'

    # mac
    db_path = './db'
    root_path = '/Users/edz/data/findata.astock.finance.sina/raw'
    for kind in ['cashflow', 'income', 'balance']:
        process_kind(kind, root_path, db_path)


if 1:
    run()
