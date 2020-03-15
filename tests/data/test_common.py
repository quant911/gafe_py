import data.common as dcom
import data.connections as conn
import unittest


class TestDBCommon(unittest.TestCase):
    def setUp(self) -> None:
        self._con = conn.get_market_data_con('PROD')

    def test_connection_alive(self):
        self.assertEqual(self._con.status, 1)

    def test_get_meta_data(self):
        res = dcom.get_table_meta_data(self._con, 'yahoo_prices')
        res0 = [{'column_name': 'ticker', 'data_type': 'character varying', 'is_nullable': 'NO'},
                {'column_name': 'data_date', 'data_type': 'date', 'is_nullable': 'NO'},
                {'column_name': 'Open', 'data_type': 'real', 'is_nullable': 'YES'},
                {'column_name': 'high', 'data_type': 'real', 'is_nullable': 'YES'},
                {'column_name': 'low', 'data_type': 'real', 'is_nullable': 'YES'},
                {'column_name': 'Close', 'data_type': 'real', 'is_nullable': 'YES'},
                {'column_name': 'Adj Close', 'data_type': 'real', 'is_nullable': 'YES'},
                {'column_name': 'committer', 'data_type': 'character varying', 'is_nullable': 'NO'},
                {'column_name': 'committs', 'data_type': 'timestamp without time zone', 'is_nullable': 'NO'}]

        self.assertListEqual(res, res0)

    def test_atomicity(self):
        # should not test on real data
        # todo: create such table
        del_sql = """ DELETE FROM test_prices WHERE data_date = '2020-01-01' """
        insert_sql = """ INSERT INTO test_prices 
            (ticker, data_date, open_price, high_price, low_price, close_price, adj_close_price, committer, commit_ts)
            VALUES
            ('AB', '2020-01-01', 114.0, 200.0, 50.0, 113.5, 113.55, 'TESTER', CURRENT_TIMESTAMP)
        """

        is_success, count = dcom.do_sql_del_n_insert(self._con, del_sql, insert_sql, is_atomic=True)

        self.assertTrue(is_success)

    def test_ideompotence(self):
        pass # call above 1, 2 times. compare the PK list