import psycopg2 as pg2

import functools as fct

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

def query_sql(con, sql, close_con=False):
    cursor = con.cursor()
    res = cursor.execute(sql)
    rows = cursor.fetchall()
    con.commit()
    cursor.close()
    if close_con:
        con.close()
    return rows