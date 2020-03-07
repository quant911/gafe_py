# manage all the available SQL connections
# DB helper packages are only imported here
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
