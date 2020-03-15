import pandas as pd
import os

import data.common as dcom
import data.df_tools as df_tools
import data.connections as conns

# must use double quote wrapping col name, or it will be converted to lower case
CREATE_YAHOO_PX_TABLE_SQL = """
    CREATE TABLE yahoo_prices (
        ticker varchar(255) not null,
        data_date date not null,
        open_price real,
        high_price real,
        low_price real,
        close_price real,
        adj_close_price real,
        volume int,
        committer varchar(255) not null,
        commit_ts timestamp not null,
        PRIMARY KEY (ticker, data_date)
    )
"""

INSERT_YAHOO_PX_TABLE_SQL_T = """
    INSERT INTO yahoo_Prices
    (ticker, data_date, open_price, high_price, low_price, close_price, adj_close_price, volume, committer, commit_ts)
    VALUES
    ('{ticker}', '{data_date}', {open_price}, {high_price}, {low_price}, {close_price}, {adj_close_price}, {volume}, 
    '{committer}', '{commit_ts}');
"""

DELETE_YAHOO_PX_TABLE_SQL_T = """
    DELETE FROM yahoo_prices
    WHERE data_date = '{data_date}' and ticker = '{ticker}'
"""

con = conns.get_market_data_con('PROD')

TICKER_UNIV = ['AB', 'C', 'BAC', 'JPM', 'UBER', 'GOOG', 'APPL', 'FB', 'BNS', 'SPY', 'QQQ']

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

    df = df.rename(columns={'Close*': 'close_price',
                            'Open': 'open_price',
                            'High': 'high_price',
                            'Low': 'low_price',
                            'Adj Close**': 'adj_close_price',
                            'Volume': 'volume',
                            'Date': 'data_date'})

    df['data_date'] = df['data_date'].astype('datetime64[ns]')
    df = df[(df['data_date'] >= data_date_from) & (df['data_date'] < data_date_to)]
    df['ticker'] = ticker

    df_px = df[~df['open_price'].str.contains('Dividend')]
    df_div = df[df['open_price'].str.contains('Dividend')]

    for col in ['open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price']:
        df_px[col] = df_px[col].astype('float64')

    for col in ['volume', ]:
        df_px[col] = df_px[col].astype('int')

    if len(df_div) > 0: # todo: apply() does not work with empty frame, how to make it un-special case??
        df_div['dividend'] = df_div.apply(lambda r: float(r['open_price'].replace(' Dividend', '')), axis=1)
        df_div = df_div[['data_date', 'ticker', 'dividend']]
    else:
        df_div = pd.DataFrame(columns=['data_date', 'ticker', 'dividend'])
    return df_px, df_div


def import_from_Yahoo(ticker, data_date, del_before_insert=True):
    # type: (str, pd.Timestamp, bool) -> (pd.DataFrame, pd.DataFrame)
    user = os.getlogin()
    df_px, df_div = query_from_Yahoo_date_range(ticker, data_date, data_date + pd.Timedelta(days=1))
    if del_before_insert:
        sql = DELETE_YAHOO_PX_TABLE_SQL_T.format(ticker=ticker, data_date=data_date.strftime('%Y-%m-%d'))
        res = dcom.do_sql(con, sql)
        print('DEL: ', res)
        print('Delete rows for {0} on {1} OK'.format(ticker, data_date))

    for rid, row in df_px.iterrows():
        sql = INSERT_YAHOO_PX_TABLE_SQL_T.format(
            ticker=ticker,
            data_date=data_date.strftime('%Y-%m-%d'),
            open_price=row['open_price'],
            high_price=row['high_price'],
            low_price=row['low_price'],
            close_price=row['close_price'],
            adj_close_price=row['adj_close_price'],
            volume=row['volume'],
            committer=user,
            commit_ts=pd.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        dcom.do_sql(con, sql)

        print('Insert rows for {0} on {1} OK'.format(ticker, data_date))

    return df_px, df_div


def do_import_from_Yahoo_one_day(data_date, del_before_insert=True):
    res = {'data_date': data_date}
    for ticker in TICKER_UNIV:
        try:
            df_px, df_div = import_from_Yahoo(ticker, data_date, del_before_insert)
            res[ticker] = len(df_px)
        except Exception as ex:
            print('ERR:', ex)
            res[ticker] = 0

    return res


def do_import_from_Yahoo_date_range(data_date_from, data_date_to, del_before_insert=True):
    res = []
    dd = data_date_from
    while dd < data_date_to:
        res.append(do_import_from_Yahoo_one_day(dd, del_before_insert))
        dd = dd + pd.Timedelta(days=1)

    return dd


def do_import_and_email(data_date_from, data_date_to, del_before_insert=True, email_receivers=None):
    pass


if __name__ == '__main__':
    df_px, df_div = import_from_Yahoo('AB', pd.to_datetime('20200221'))
    print(df_px)
    print(df_div)


