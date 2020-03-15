import logging
import pandas as pd
import numpy as np

import functools as fct
import collections as coll
logger = logging.getLogger(__file__)


def do_sql(con, sql):
    # perform update or insert or delete, or anything
    # todo: review PostgreSql own stuff
    # https://www.postgresqltutorial.com/postgresql-upsert/
    cursor = con.cursor()
    cursor.execute(sql)
    con.commit()
    rowcount = cursor.rowcount
    cursor.close()

    return rowcount


def do_sql_insert_df(con, df, table_name, col_to_field_dic=None):
    # perform bulk insert
    # if col_to_field_dic is None, assume all df columns should write to table and table field names match df col names
    import io
    buff = io.StringIO()

    if col_to_field_dic is None:
        df[col_to_field_dic.keys()].to_csv(buff, sep='\t', header=False, index=False)
    else:
        df.to_csv(buff, sep='\t', header=False, index=False, columns=col_to_field_dic.keys())
    #buff.getvalue()
    buff.seek(0)

    cursor = con.cursor()
    cursor.copy_from(buff, table_name, null='', columns=col_to_field_dic.values())
    con.commit()
    rowcount = cursor.rowcount
    cursor.close()

    return rowcount


def do_sql_query(con, sql, close_con=False):
    # should obtain meta data to generate proper cols if no row
    cursor = con.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    con.commit()
    cursor.close()
    if close_con:
        con.close()
    return rows


PG_TYPE_TO_NP_DTYPE_DIC = {
    'boolean': bool,
    'date': 'timestamp[ns64]',
    'double': float,
    'integer': int,

}


@fct.lru_cache(1000)
def get_table_meta_data(con, table_name):
    sql = """
        SELECT column_name, data_type, is_nullable FROM information_schema.columns
        WHERE  table_schema NOT IN ('information_schema', 'pg_catalog')
        AND table_name = '{0}'
    """.format(table_name)

    res = do_sql_query(con, sql)
    res = [{ 'column_name': row[0], 'data_type': row[1], 'is_nullable': row[2]} for row in res]
    return res


@fct.lru_cache(1000)
def get_dtypes_from_table_meta_data(con, table_name):
    meta_data = get_table_meta_data(con, table_name)
    dtype_dic = {}

    for m in meta_data:
        pg_type = m['data_type']
        np_dtype = object
        if pg_type == 'boolean':
            np_dtype = np.bool
        elif pg_type in ('date', 'timestamp without time zone'):
            np_dtype = 'datetime64[ns]'
        elif pg_type == 'real':
            np_dtype = np.float64
        elif pg_type == 'character varying':
            np_dtype = str
        else:
            logger.warning('Unmapped PostgreSql type: {0}, mapping to object for now'.format(pg_type))

        dtype_dic[m['column_name']] = np_dtype

    return dtype_dic


def type_code_to_np_dtype(tc):
    # type_code is as defined in cursor.description.Column.type_code
    if tc == 1043:
        return str
    if tc == 700:
        return np.float64
    if tc == 1114:
        return 'datetime64[ns]'
    if tc == 1082:
        return 'datetime64[ns]'
    return object

def create_typed_empty_dataframe(dtype_dic):
    series = [pd.Series(name=k, dtype=v) for k, v in dtype_dic.items()]
    df = pd.concat(series, axis=1)

    return df


def create_typed_empty_datafram_using_table_meta_data(con, table_name):
    dtype_dic = get_dtypes_from_table_meta_data(con, table_name)
    return create_typed_empty_dataframe(dtype_dic)


def is_single_table_sql(sql):
    # type: (str) -> bool
    return 'join' not in sql.lower()


def infer_table_name_from_sql(sql):
    sql = sql.lower()
    if not is_single_table_sql(sql):
        raise ValueError('infer_table_name_from_sql() only works for single table query: {0}'.format(sql))
    tokens = sql.lower().split(' ')
    return tokens[1+tokens.index('from')]


def do_sql_query_df(con, sql, close_con=False, infer_dtypes=True):
    # type: (Connection, str, bool, bool) -> pd.DataFrame
    # convert query result to dataframe
    # infer_dtypes: use DB meta data to imply DataFrame column dtype
    #
    # -- works for single table query for now
    # 2) cannot handle alias

    cursor = con.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()

    dtypes = coll.OrderedDict([(d.name, type_code_to_np_dtype(d.type_code)) for d in cursor.description])
    if len(data) > 0:
        df = pd.DataFrame(data, columns=dtypes.keys())
        for col in df.columns:
            df[col] = df[col].astype(dtypes[col])
    else:
        if infer_dtypes:
            table_name = infer_table_name_from_sql(sql)
            df = create_typed_empty_datafram_using_table_meta_data(con, table_name)
        else:
            df = pd.DataFrame(data) # fall back for multi-table query
    return df


def infer_query_sql_from_del_sql(sql):
    # type: (str) -> str
    # transform a DELETE stmt to a SELECt query,
    # which allows saving down results before save
    # sample input
    # DELECT FROM table xyz
    # WHERE <clauses>
    sql = sql.lower()
    if 'delete from' not in sql:
        raise ValueError('Unsupported DELETE stmt')

    new_sql = sql.replace('delete from', 'select * from')
    return new_sql


def do_sql_del_n_insert(con, del_sql, ins_sql, max_backup_rows=100000, is_atomic=True, _raise_after_del=False):
    # type: (Connection, str, str, int, bool, bool) -> (bool, int)
    #
    # this function does not throw exception
    # if is_atomic is true, the system must be in one of the two states upon exit:
    # 1) both del and insert succeed, function returns (True, insert_row_count)
    # 2) either del or insert or sth else fails, function returns (False, 0)
    #
    # this function has a few params starting with '_' for internal testing purpose
    # _raise_after_del: raise an exception after deleting old content; turn it on to test back up logic
    backup_df = None
    if not is_single_table_sql(ins_sql):
        logger.error('do_sql_del_n_insert() only works for single table query, {0}'.format(ins_sql))
        return False, 0

    if is_atomic:
        backup_sql = infer_query_sql_from_del_sql(del_sql)
        try:
            backup_df = do_sql_query_df(con, backup_sql, close_con=False, infer_dtypes=True)
        except Exception as ex:
            logger.error('do_sql_del_n_insert() fails to back up rows. Abort.')
            logger.error('del sql is:',del_sql)
            return False, 0

    is_success = True
    try:
        del_status = do_sql_delete(con, del_sql)
    except Exception as ex:
        logger.error('del_n_insert() fails when deleting rows. Abort')
        is_success = False

    if is_success:
        ins_status = do_sql_insert(con, ins_sql)
        if ins_status != 0: #todo: how to detect error status??
            do_sql_insert_df(con, backup_df)
        else:
            return True, -1 #todo: update -1 to actual row count returned
    else:
        return False, 0

