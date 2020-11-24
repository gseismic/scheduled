import os
from collections import defaultdict
from finapi.common.filecache import filecache
#from finapi.common.filecache import filecache_static
from report_utils import get_classified_fields, zh_to_py, zh_to_shortpy
# from filecache import filecache


def get_union_n_common(root_path, kind, verbose=1):
    # cashflow等特定报表，银行、证券等字段合并
    # full_fields: 各个股票对应的列表
    data_path = os.path.join(root_path, kind)
    full_fields = get_classified_fields(data_path)
    common = set(list(full_fields.keys())[0])
    union = set()
    for fields, symbols in full_fields.items():
        common = common & set(fields)
        union |= set(fields)
        if verbose:
            print('#'*50)
            print('field', fields, len(fields))
            print(symbols[:30], '..')

    if verbose >= 1:
        print('='*100)
        print('common:')
        print(common, len(common))
        print('='*100)
        print('union:')
        print(union, len(union))

    if verbose >= 1:
        print('='*100)
        for fields, symbols in full_fields.items():
            print('#'*50)
            diff = set(fields) -common
            print('diff:')
            print(diff)
            print(symbols[:30], '..')

    return full_fields, common, union



def run_fetch_all_fields(root_path, kind, data_dir):
    print('='*50, kind, '='*50)
    os.makedirs(data_dir, exist_ok=True)
    full_fields, common, union = get_union_n_common(root_path, kind, verbose=1)

    # store fields
    idx = 0
    for fields, symbols in full_fields.items():
        shortpy_list = [zh_to_shortpy(f) for f in fields]
        print(fields)
        print(shortpy_list)
        assert(len(shortpy_list) == len(set(shortpy_list)))
        base_ = os.path.join(data_dir, kind + str(idx))
        with open(base_ + '.fields.list', 'w') as f:
            text = '\n'.join(['%s,%s' % (f, py) for f, py 
                              in zip(fields, shortpy_list)])
            f.write(text)
        with open(base_ + '.symbols.list', 'w') as f:
            f.write('\n'.join(symbols))
        idx += 1

    pinyin_fields = []
    pyshort_dictlist = defaultdict(list) # 首字母简写
    for key in union:
        py = zh_to_py(key)
        dic = {}
        dic['zh'] = key
        dic['pinyin'] = '_'.join(py)
        dic['py'] = ''.join([ii[0].upper() for ii in py])
        if dic['py'] in pyshort_dictlist:
            print('!'*5, dic)
        pyshort_dictlist[dic['py']].append(key)
        pinyin_fields.append(dic)
        print(dic['py'], dic['zh'])

    print('&'*20)
    for py, items in pyshort_dictlist.items():
        if len(items) != 1:
            print(py, items)

    print(len(set([dic['py'] for dic in pinyin_fields])),
          len([dic['py'] for dic in pinyin_fields]))
    assert(len(set([dic['py'] for dic in pinyin_fields])) 
           == len([dic['py'] for dic in pinyin_fields]))

    out_file = os.path.join(data_dir, kind)
    with open(out_file, 'w') as f:
        text = '\n'.join(['%-70s, %-20s, %-50s' % (ii['pinyin'], ii['py'],
                                                 ii['zh']) 
                          for ii in pinyin_fields])
        f.write(text)
        text = '\n'.join(['%-70s %-20s %-50s' % (ii['pinyin'], ii['py'],
                                                 ii['zh']) 
                          for ii in pinyin_fields])
        print(text)


def run():
    # read
    root_path = '/media/lsl/untitled/findata/astock/finance/sina/raw'
    root_path = '/Users/edz/data/findata.astock.finance.sina/raw'
    data_dir = './data'
    for kind in ['balance', 'income', 'cashflow']:
        run_fetch_all_fields(root_path, kind, data_dir)


if 1:
    run()
