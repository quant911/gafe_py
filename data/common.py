import psycopg2 as pg2
import logging
import pandas as pd
import numpy as np

import functools as fct

logger = logging.getLogger(__file__)

CON_DIC_PROD = {'azure-marketdata': {'host': 'norfolk2.postgres.database.azure.com',
                  'dbname': 'rawmarketdatadb', # case sensitive!
                  'user': 'fengn@norfolk2',
                  'password': 'typecats123!', # this is saved to github hence publicly viewable!!!
                  'sslmode': 'require'},
        }


def get_con_dic(env):
    if env == 'PROD':
        return CON_DIC_PROD
    else:
        raise ValueError('env={0} not impl'.format(env))


@fct.lru_cache(maxsize=10)
def get_market_data_con(env='PROD'):
    con_dic = get_con_dic(env)

    dic = con_dic['azure-marketdata']
    con_str = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(
        dic['host'], dic['user'], dic['dbname'], dic['password'], dic['sslmode'])
    con = pg2.connect(con_str)
    return con


def insert_sql(con, sql, close_con=False):
    cursor = con.cursor()
    res = cursor.execute(sql)
    con.commit()
    cursor.close()
    if close_con:
        con.close()

    return res


def delete_sql(con, sql, close_con=False):
    cursor = con.cursor()
    res = cursor.execute(sql)
    con.commit()
    cursor.close()
    if close_con:
        con.close()

    return res


def query_sql(con, sql, close_con=False):
    # should obtain meta data to generate proper cols if no row
    cursor = con.cursor()
    res = cursor.execute(sql)
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

    res = query_sql(con, sql)
    res = [{ 'column_name': row[0], 'data_type': row[1], 'is_nullable': row[2]} for row in res]
    return res


@fct.lru_cache(1000)
def get_table_meta_to_dtypes(con, table_name):
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


def create_typed_empty_dataframe(dtype_dic):
    series = [pd.Series(name=k, dtype=v) for k, v in dtype_dic.items()]
    df = pd.concat(series, axis=1)

    return df

def query_sql_dataframe(con, sql, close_con=False, infer_dtypes=True):
    # convert query result to dataframe
    # infer_dtypes: use DB meta data to imply DataFrame column dtype
    #
    # -- works for single table query for now
    # 2) cannot handle alias

    data = query_sql(con, sql, close_con)
    if len(data) > 0:
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame(get_table_meta_data(con, table_name))

def del_to_query(sql):
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


def del_n_insert(con, del_sql, ins_sql, max_backup_rows=100000, is_atomic=True):
    # type: (str, str, int, bool) -> (bool, int)
    # if is_atomic is true, the system must be in one of the two states:
    # 1) both del and insert succeed, function returns (True, insert_row_count)
    # 2) either del or insert or sth else fails, function returns (False, 0)
    backup_df = None
    if is_atomic:
        backup_sql = del_to_query(del_sql)
        try:
            backup_df = query_sql(con, backup_sql) #todo: does it really return DataFrame?
        except Exception as ex:
            logger.error('del_n_insert() fails to back up rows. Abort.')
            logger.error('del sql is:',del_sql)
            return False, 0

    is_success = True
    try:
        del_status = delete_sql(con, del_sql)
    except Exception as ex:
        logger.error('del_n_insert() fails when deleting rows. Abort')
        is_success = False
        #todo: restore based on del_status

    return True, -1 #todo: update -1 to actual row count returned

