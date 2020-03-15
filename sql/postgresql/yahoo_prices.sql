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