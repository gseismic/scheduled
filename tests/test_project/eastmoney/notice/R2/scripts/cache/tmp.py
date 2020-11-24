
    # print(fields)
    for column in df:
        if column == '报表日期':
            continue
        # print(df[column])
        # print(len(fields), len(df[column]))
        assert(len(fields) == len(df[column]))

        is_separator = all([is_nan(value) for value in df[column]])
        # print(all([is_nan(value) for value in df[column][1:]]))
        assert(is_separator == False)
        dic = OrderedDict()
        for ii, f in enumerate(fields):
            shortpy = zh_to_shortpy(f) 
            value = df[column][ii]
            #print(type(value))
            #print(value)
            #print(value == np.nan)
            if value is np.nan or value != value:
                # is nan
                # assert(0)
                pass
            #if np.isnan(value):
            # assert(value)
            dic[shortpy] = {'name': f, 'value': value}
        #print(dic)
        # print(df)



def process_kind_symbol_old(kind, sector, symbol, df, fields, shortpy_list):
    print(df)
    db_path = './db'
    os.makedirs(db_path, exist_ok=True)
    dbfile = os.path.join(db_path, '%s.sqlite3' % (sector))

    conn = sqlite3.connect(dbfile)
    conn.execute('pragma journal_mode=wal;')

    sql_create = 'create table if not exists %s' % kind

    valid_fields = []
    didict = defaultdict(dict)

    columns = df.columns[1:]
    for index, row in df.iterrows():
        # print('row', index, row, len(row))
        # 分界线，流动资产 http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/600003/ctrl/part/displaytype/4.phtml
        is_separator = all([is_nan(value) for value in row[1:]])
        if is_separator:
            continue

        field = row[0]
        valid_fields.append(field)
        abbr = zh_to_shortpy(field)
        # didict[abbr]['date'] = 
        for icol, colval in enumerate(row):
            if icol == 0:
                colname = 'name'
                didict[colname][abbr] = colval
            elif colval in ['元', '万元']:
                colname = df.columns[icol]
                didict[colname][abbr] = colval
            else:
                colname = df.columns[icol]
                didict[colname][abbr] = float(colval)

        for colname, dic in didict.items():
            print(colname)
            print(dic)
    # print(didict)

    try:
        pass
    finally:
        conn.close()
