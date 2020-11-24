import time
import glob
import pandas as pd
from collections import defaultdict
from finapi.common.filecache import filecache_static
from finapi.common.filecache import filecache
# from filecache import filecache


root_path = '/media/lsl/untitled/findata/astock/finance/sina/raw/cashflow'


@filecache(3*60*60)
def get_classified_fields(root_path):
    # 银行，保险，证券，普通
    # YH, BX, ZQ, PT
    filenames = sorted(glob.glob(root_path + '/*.csv'))
    full_fields = defaultdict(list)
    for i, fn in enumerate(filenames):
        print(i, fn)
        df = pd.read_csv(fn)
        fields = tuple(df['报表日期'])
        assert(len(list(fields)) == len(set(fields))) # OK
        full_fields[fields].append(fn.split('/')[-1])
    return full_fields


def run():
    # print('all_fields', all_fields)
    # 除了命名上更改，一切尊重原始数据
    full_fields = get_classified_fields(root_path)
    common = set(list(full_fields.keys())[0])
    union = set()
    for fields, symbols in full_fields.items():
        pass



if 1:
    run()
