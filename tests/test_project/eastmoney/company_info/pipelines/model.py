

table_fields = [
    {'name': 'symbol', 'dtype': 'CHAR(10)', 'index': True, 'nullable': False, 'primary': True},
    {'name': 'company_name', 'dtype': 'text', 'nullable': False},
    {'name': 'company_name_en', 'dtype': 'datetime', 'index': True, 'primary': True},
    {'name': 'founded_date', 'dtype': 'text', 'index': True},
    {'name': 'ipo_date', 'dtype': 'text', 'index': True},
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


basicinfo_table = [
    {'name': 'name', 'dtype': 'CHAR(10)'},
]


flatten_dict = {
    'name': 'jbzl.gsmc',
    'name_en': 'jbzl.ywmc',
    'name_history': 'jbzl.cym',
    'A_code': 'jbzl.agdm',
    'A_name': 'jbzl.agjc',
    'B_code': 'jbzl.bgdm',
    'B_name': 'jbzl.bgjc',
    'H_code': 'jbzl.hgdm',
    'H_name': 'jbzl.hgjc',
    'security_type': 'jbzl.zqlb',
    'em_industry': 'jbzl.sshy',
    'ipo_exchange': 'jbzl.ssjys',
    'zjh_industry': 'jbzl.sszjhhy',
    'ceo': 'jbzl.zjl',
    'juridical_leader': 'jbzl.frdb', # fa-ren-dai-biao
    'fax': 'jbzl.cz',
    'website': 'jbzl.gswz',
    'email': 'jbzl.dzxx',
    'registered_address': 'jbzl.zcdz',
    'registered_capital': 'jbzl.zczj',
    'registered_code': 'jbzl.gsdj',
    'accounting_firm': 'jbzl.kjssw',
    'office_address': 'jbzl.bgdz',
    'location': 'jbzl.qy',
    'postcode': 'jbzl.yzbm',
    'CodeType': 'CodeType',
    'symbol': 'Code',
    'company_intro': 'jbzl.gsjj',
    'business_scope': 'jbzl.jyfw',
}


def dict_nest_to_flat(flatten_dict):
    dbdict = {}
    for key, key2 in flatten_dict.items():
        nest_key = key2.split('.')
        if len(nest_key) == 1:
            dbdict[key] = data[nest_key]
        elif len(nest_key) == 2:
            dbdict[key] = data[nest_key[0]][nest_key[1]]
        elif len(nest_key) == 3:
            dbdict[key] = data[nest_key[0]][nest_key[1]][nest_key[2]]
        else:
            assert 0, 'NEVER HAPPEN'
    return dbdict
