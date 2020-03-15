-- dummy price table used only by testing code
-- schema is identical to yahoo_prices table
 CREATE TABLE test_prices (
    ticker varchar(255) not null,
    data_date date not null,
    open_price real,
    high_price real,
    low_price real,
    close_price real,
    adj_close_price real,
    committer varchar(255) not null,
    commit_ts timestamp not null,
    PRIMARY KEY (ticker, data_date)
    )