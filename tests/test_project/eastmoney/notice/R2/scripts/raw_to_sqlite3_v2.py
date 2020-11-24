import os
import glob
import pandas as pd
from collections import defaultdict, OrderedDict
#from finapi.common.filecache import filecache_static
from report_utils import process_field, zh_to_shortpy, get_sector_type, normalize_sheet
from sqliteutil.v2 import Database, Table


pd.set_option('display.max_rows', None)


def build_sqlite_values(df, kind, symbol, db_tag='finance_sina'):
    table_dict = [
        {'name': 'symbol', 'dtype': 'text', 'nullable': False, 'primary': True},
        {'name': 'findate', 'dtype': 'text', 'nullable': False,  'primary': True},
        {'name': 'findate_type', 'dtype': 'text', 'nullable': False}
    ]

    for column in df.columns:
        table_dict.append({'name': column, 'dtype': 'real'})

    db_many_values = defaultdict(list)
    for index, row in df.iterrows():
        dic = OrderedDict()
        dic['findate'] = index
        dic['findate_type'] = index[4:]
        dic['symbol'] = symbol.upper()
        for k, val in row.items():
            dic[k] = val
        
        dbname = '%s.%ss.sqlite3' % (db_tag, int(dic['findate'][:4])//10 * 10)
        db_many_values[dbname].append(dic)

    return table_dict, db_many_values


def process_kind_symbol(kind, sector, symbol, df, db_path):
    os.makedirs(db_path, exist_ok=True)

    df, columns_zh = normalize_sheet(df)
    if df is None:
        return

    table_dict, db_many_values = build_sqlite_values(df, kind, symbol, db_tag='finance_sina')

    for dbname, many_values in db_many_values.items():
        dbfile = os.path.join(db_path, dbname)
        db = Database(dbfile, echo=True)
        db.execute('pragma journal_mode=wal;')
        try:
            tbl_name = '%s_%s' % (kind, sector.lower())
            table = Table(db, tbl_name, table_dict, echo=True)
            table.create_table(ignore_if_exists=True)
            table.dict_insertmany(many_values, action='insert or ignore into')
        finally:
            db.commit()
            db.close()


def process_kind(kind, root_path, db_path):
    datadir = root_path.rstrip('/') + '/' + kind + '/*.csv'
    filenames = sorted(glob.glob(datadir))
    for i, fn in enumerate(filenames):
        print(i, fn)
        df = pd.read_csv(fn)
        fields = tuple([process_field(f) for f in df['报表日期']])
        # shortpy_list = [zh_to_shortpy(f) for f in fields]
        sector = get_sector_type(kind, fields)
        symbol = fn.split('/')[-1].split('.')[0]
        # print(symbol, sector)
        process_kind_symbol(kind, sector, symbol, df, db_path)


def run():
    # 一切尊重原始数据，保持一一映射
    root_path = '/media/lsl/untitled/findata/astock/finance/sina/raw'
    # data_dir = './data'

    # mac
    db_path = './db_new'
    root_path = '/Users/edz/data/findata.astock.finance.sina/raw'
    for kind in ['income', 'cashflow', 'balance']:
        process_kind(kind, root_path, db_path)


if 1:
    run()
