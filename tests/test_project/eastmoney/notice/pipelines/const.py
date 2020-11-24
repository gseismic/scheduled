# ['code', 'name', 'content', 'range_change', 'result_type', 'profit_last_year', 'type', 'publish_date', 'finance_date']
"""
'publish_date': '2014-06-03T00:00:00+08:00', 'title': '601877 : 正泰电器股权激励计划第一期第二次行权结果暨股份上市公告', 'info_code': 'AN201406020005960459', 'attach_type': 'pdf', 'sec_code': '601877', 'sectype_code': '058001001', 'name': '正泰电器', 'trade_market': '上交所主板', 'trade_market_code': '069001001001', 'listing_state': '0', '_ENDDATE': '2014-06-03T00:00:00+08:00', 'EUTIME': '2014-06-02T16:57:56+08:00', 'report_type_code': '001002007001004', 'report_type_name': '股权激励行权结果', 'detail_url': 'http://data.eastmoney.com/notices/detail/601877/AN201406020005960459,JWU2JWFkJWEzJWU2JWIzJWIwJWU3JTk0JWI1JWU1JTk5JWE4.html', 'may_pdf_url': 'https://notice.eastmoney.com/pdffile/web/H2_AN201406020005960459_1.pdf

publish_date: 对于 eutime
"""
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
