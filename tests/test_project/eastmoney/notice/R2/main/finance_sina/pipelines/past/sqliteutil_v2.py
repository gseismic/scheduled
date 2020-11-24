
"""
db_keys = [
    {'name': 'name', 'dtype': 'text', 'index': False, 'unique': False, 'nullable': True}
]
"""


def commit_or_rollback_raise(conn):
    try:
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def get_createtable_sql(table_name, db_keys, only_if_not_exists):
    if only_if_not_exists:
        sql = 'CREATE TABLE IF NOT EXISTS %s(' % table_name
    else:
        sql = 'CREATE TABLE %s(' % table_name

    primary_keys = []
    for item in db_keys:
        sql += '%s %s' % (item['name'], item['dtype'])
        nullable = item.get('nullable', True)
        if not nullable:
            sql += ' NOT NULL'

        unique = item.get('unique', False)
        if unique:
            sql += ' UNIQUE'

        default = item.get('default')
        if default:
            sql += ' DEFAULT ' + str(default)

        primary = item.get('primary', False)
        if primary:
            primary_keys.append(item['name'])

        sql += ','

    if primary_keys:
        sql += 'PRIMARY KEY(' + ','.join(primary_keys) + ')'
    else:
        sql = sql.rstrip(',')
    sql += ')'
    return sql


def get_index_fields(table_name, db_keys):
    index_fields = []
    for item in db_keys:
        if item.get('index', False):
            index_fields.append(item['name'])
    return index_fields


def safe_execute_commit_sql(conn, cur, sql, echo=False):
    if echo:
        print('SQL: %s' % sql)
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def try_create_table(conn, cur, table_name, db_keys, create_index=True,
                 only_if_not_exists=True, echo=False):
    sql = get_createtable_sql(table_name, db_keys, only_if_not_exists)
    safe_execute_commit_sql(conn, cur, sql, echo=echo)
    if create_index:
        index_fields = get_index_fields(table_name, db_keys)
        for field in index_fields:
            try_create_index(conn, cur, table_name, field,
                             only_if_not_exists=only_if_not_exists,
                             echo=echo)


def try_create_index(conn, cur, table_name, field, option='ASC',
                 only_if_not_exists=True, echo=False):
    # print('Try creating index ...')
    if only_if_not_exists:
        base_sql = 'CREATE INDEX IF NOT EXISTS'
    else:
        base_sql = 'CREATE INDEX'

    sql = '%s %s_index on %s (%s ' % (base_sql, field, table_name, field)
    if option:
        sql += option

    sql += ')'
    safe_execute_commit_sql(conn, cur, sql, echo=echo)


def insert_many(conn, cur, table_name, action, list_values, commit=True,
                echo=False):
    if not list_values:
        return
    num_fields = len(list_values[0])
    sql = '%s %s values (%s)' % (action, table_name, 
                                 ','.join(['?'] * num_fields))

    if echo:
        print(sql)

    cur.executemany(sql, list_values)
    if commit:
        commit_or_rollback_raise(conn)
