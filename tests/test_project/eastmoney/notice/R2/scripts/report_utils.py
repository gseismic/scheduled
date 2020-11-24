import glob
import string
import numpy as np
import pandas as pd
from collections import defaultdict
from finapi.common.filecache import filecache
from pypinyin import pinyin, lazy_pinyin, Style


def process_field(field):
    field = field.replace('：', ':').replace('加:', '')
    field = field.replace('减:', '')

    for num in ['一', '二', '三', '四', '五', 
                '六', '七', '八', '九', '十',
                '十一', '十二', '十三', '十四',
                '十五', '十六', '十七', '十八']:
        field = field.replace('%s、' % num, '%s_' % num)
    return field


def zh_to_shortpy(key):
    py = zh_to_py(key)
    return ''.join([ii[0].upper() for ii in py])


def zh_to_py(key):
    # YiSZK, 存储为YISZK，查询时upper
    _key = key.replace('预收', '预v收')
    _key = _key.replace('应收', '应i收')
    _key = _key.replace('库藏', '库藏a')
    _key = _key.replace('库存', '库存u')
    _key = _key.replace('汇兑收益', '汇兑收o益')
    _key = _key.replace('汇兑损益', '汇兑损u益')
    _key = _key.replace('公允价值变动收益', '公允价值变动收o益')
    _key = _key.replace('公允价值变动损益', '公允价值变动损u益')
    _key = _key.replace('少数股东收益', '少数股东收o益')
    _key = _key.replace('少数股东损益', '少数股东损u益')
    #_key = _key.replace('其中:', 'QZ_')
    _key = _key.replace('、', '_')
    _key = _key.replace('：', '_')
    _key = _key.replace(':', '_')
    _key = _key.replace('/', '_')
    _key = _key.replace('（', '(').replace('）', ')')
    _key = _key.replace('(', '_').replace(')', '_')
    _key = _key.replace('<', '_').replace('>', '_')
    _key = _key.strip('_').replace('__', '_')
    py = lazy_pinyin(_key)
    allowed = string.ascii_letters + '_'
    # print([i in allowed for i in allowed])
    assert(all([i in allowed for i in allowed]))
    return py


@filecache(72*60*60)
def get_classified_fields(data_path):
    # 银行，保险，证券，普通
    # YH, BX, ZQ, PT
    filenames = sorted(glob.glob(data_path + '/*.csv'))
    full_fields = defaultdict(list)
    for i, fn in enumerate(filenames):
        print(i, fn)
        df = pd.read_csv(fn)
        fields = tuple(df['报表日期'])
        fields = tuple([process_field(f) for f in fields])

        shortpy_list = [zh_to_shortpy(f) for f in fields]
        # print(shortpy_list)
        assert(len(shortpy_list) == len(set(shortpy_list)))
        # print(fields)
        assert(len(list(fields)) == len(set(fields))) # OK
        full_fields[fields].append(fn.split('/')[-1].split('.')[0])
    return full_fields


def get_sector_type(kind, fields):
    if kind == 'balance':
        if '存放同业款项' in fields:
            sector = 'YH'
        elif '代理承销证券款' in fields:
            sector = 'ZQ'
        elif '应付保单红利' in fields:
            sector = 'BX'
        else:
            sector = 'PT'
    elif kind == 'cashflow':
        if '向央行借款净增加额' in fields:
            sector = 'YH'
        elif '支付利息、手续费及佣金的现金' in fields:
            sector = 'ZQ'
        elif '自动垫缴保费收入' in fields:
            sector = 'BX'
        else:
            sector = 'PT'
    elif kind == 'income':
        if '代理买卖证券业务净收入' in fields:
            sector = 'ZQ'
        elif '已赚保费' in fields:
            sector = 'BX'
        elif '利息净收入' in fields:
            # 证券里也有
            sector = 'YH'
        else:
            sector = 'PT'
    else:
        raise Exception('get_sector_type')
    return sector


def normalize_sheet(df):
    df = df.transpose()
    df.columns = df.iloc[0]
    df.drop(['报表日期'], axis=0, inplace=True)
    col_drop = []
    is_nan = lambda value: value is np.nan or value != value
    for column in df.columns:
        if all([is_nan(_) for _ in df[column]]):
            col_drop.append(column)
        df[column] = pd.to_numeric(df[column], errors='ignore', downcast='float')
    # drop分界线，流动资产 http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/600003/ctrl/part/displaytype/4.phtml
    df.drop(col_drop, axis=1, inplace=True)

    # 转换单位：统一为元
    for index, row in df.iterrows():
        # print(index, row)
        unit = row['单位']
        if unit == '元':
            pass
        elif unit == '万元':
            for i in range(1, len(row)):
                row[i] *= 10000
        else:
            assert(0)

    if df.empty:
        return None, None

    # print(df)
    df.drop(['单位'], axis=1, inplace=True)
    columns_zh = list(df.columns)
    df.columns = [zh_to_shortpy(col) for col in df.columns]
    return df, columns_zh
