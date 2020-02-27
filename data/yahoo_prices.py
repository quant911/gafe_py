import pandas as pd
import os
import psycopg2 as pg2

import data.common as dcom


# must use double quote wrapping col name, or it will be converted to lower case
CREATE_YAHOO_PX_TABLE_SQL = """
    CREATE TABLE Yahoo_Prices (
    "Ticker" varchar(255) not null,
    "DataDate" date not null,
    "Open" real,
    "High" real,
    "Low" real,
    "Close" real,
    "Adj Close" real,
    "Committer" varchar(255) not null,
    "CommitTS" timestamp not null,
    PRIMARY KEY (Ticker, Data_date)
    )
"""

INSERT_YAHOO_PX_TABLE_SQL_T = """
    INSERT INTO Yahoo_Prices
    ()
    VALUES
    ('{0}', '{1}', {2}, {3}, {4}, {5}, {6}, '{7}', '{8}');
"""

DELETE_YAHOO_PX_TABLE_SQL_T = """
    DELETE FROM Yahoo_Prices
    WHERE Date_date = '{0}' and Ticker = '{1}'
"""

con = dcom.get_market_data_con('PROD')


def create_Yahoo_tables_with_tmp():
    # tmp table is 1-to-1 created to hold INSERTION tmp result
    # facilitate ideoponent del-n-insert
    #1. create Yahoo_Price table
    #2. create Yahoo_Dividend table


def query_from_Yahoo_date_range(ticker, data_date_from, data_date_to):
    # type: (str, pd.Timestamp, pd.Timestamp) -> (pd.DataFrame, pd.DataFrame)
    # return both prices and events
    # prices are separated from event to allow easier manipulation in downstream
    # the dates admitted are [data_date_from, data_date_to)

    url = r'https://finance.yahoo.com/quote/{0}/history?p={0}'.format(ticker)
    raw_data = pd.read_html(url)
    df = raw_data[0]
    df = df[: -1] # remove the "summary" row in bottom
    if type(df) != pd.DataFrame:
        raise ValueError('Parsing from Yahoo Finance fails: {0}'.format(url))

    df = df.rename(columns={'Close*': 'Close',
                            'Adj Close**': 'Adj Close',
                            'Date': 'Data_date'})

    df['Data_date'] = df['Data_date'].astype('datetime64[ns]')
    df = df[(df['Data_date'] >= data_date_from) & (df['Data_date'] < data_date_to)]
    df['Ticker'] = ticker

    df_px = df[~df['Open'].str.contains('Dividend')]
    df_div = df[df['Open'].str.contains('Dividend')]

    for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
        df_px[col] = df_px[col].astype('float')

    for col in ['Volume', ]:
        df_px[col] = df_px[col].astype('int')

    df_div['Dividend'] = df_div.apply(lambda r: float(r['Open'].replace(' Dividend', '')), axis=1)
    df_div = df_div[['Data_date', 'Ticker', 'Dividend']]
    return df_px, df_div


def import_from_Yahoo(ticker, data_date, del_before_insert=True):
    # type: (str, pd.Timestamp, bool) -> (pd.DataFrame, pd.DataFrame)
    user = os.getlogin()
    df_px, df_div = query_from_Yahoo_date_range(ticker, data_date, data_date + pd.Timedelta(days=1))
    if del_before_insert:
        sql = DELETE_YAHOO_PX_TABLE_SQL_T.format(ticker, data_date.strftime('%Y-%m-%d'))
        res = dcom.delete_sql(con, sql)
        print('DEL: ', res)
        print('Delete rows for {0} on {1} OK'.format(ticker, data_date))

    for row in df_px.iterrows():
        sql = INSERT_YAHOO_PX_TABLE_SQL_T.format(ticker, data_date.strftime('%Y-%m-%d'),
                                             row['Open'], row['High'], row['Low'],
                                            row['Close'], row['Adj Close'],
                                                 user, pd.datetime.now().strftime('%Y-%m-%d %H:%m:%s'))
        dcom.insert_sql(con, sql)

        print('Insert rows for {0} on {1} OK'.format(ticker, data_date))

    return df_px, df_div


if __name__ == '__main__':
    df_px, df_div = import_from_Yahoo('AB', pd.to_datetime('20200221'))
    print(df_px)
    print(df_div)


