import pandas as pd

CREATE_YAHOO_PX_TABLE_SQL = """
    CREATE TABLE Yahoo_Prices (
    Ticker varchar(255) not null,
    Data_date date not null,
    "Open" real,
    High real,
    Low real,
    "Close" real,
    "Adj Close" real,
    Committer varchar(255) not null,
    CommitTS timestamp not null,
    PRIMARY KEY (Ticker, Data_date)
    )
"""


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
    df = df[(df['Date'] >= data_date_from) & (df['Date'] < data_date_to)]
    df['Ticker'] = ticker

    df_px = df[~df['Open'].str.contains('Dividend')]
    df_div = df[df['Open'].str.contains('Dividend')]

    for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
        df_px[col] = df_px[col].astype('float')

    for col in ['Volume', ]:
        df_px[col] = df_px[col].astype('int')

    df_div['Dividend'] = df_div.apply(lambda r: float(r['Open'].replace(' Dividend', '')), axis=1)
    df_div = df_div[['Date', 'Ticker', 'Dividend']]
    return df_px, df_div


def import_from_Yahoo(ticker, data_date, del_before_insert=True):
    # type: (str, pd.Timestamp) -> (pd.DataFrame, pd.DataFrame)
    df_px, df_div = query_from_Yahoo_date_range(ticker, data_date, data_date + pd.Timedelta(days=1))
    if del_before_insert:
        print('Delete rows for {0} on {1} OK'.format(ticker, data_date))
    else:
        print('Insert rows for {0} on {1} OK'.format(ticker, data_date))



if __name__ == '__main__':
    df_px, df_div = import_from_Yahoo('AB', pd.to_datetime('20200221'))
    print(df_px)
    print(df_div)


