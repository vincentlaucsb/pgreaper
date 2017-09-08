''' Tests regarding converting SQLite databases to Postgres databases '''

import pgreaper
from pgreaper.testing import *
from pgreaper.postgres.conn import postgres_connect
from pgreaper.sqlite import to_postgres

import sqlite3
            
class SQLiteToPGTest(PostgresTestCase):
    ''' Test if SQLite to Postgres conversion works '''
    
    drop_tables = ['random_numbers']
    
    @classmethod
    def setUpClass(cls):
        ''' Convert a SQLite database of random numbers to Postgres '''
        pgreaper.sqlite_to_postgres(
            sqlite_db=os.path.join(DATA_DIR, 'sqlite_numbers.db'),
            dbname=TEST_DB,
            name='random_numbers')
        
    def test_count(self):
        self.assertCount('random_numbers', 500)
        
    def test_col_names(self):
        self.assertColumnNames('random_numbers',
            ['uniform0_1', 'uniform0_10', 'stdnorm', 'norm5_25', 'exp_1'])
        
    def test_col_types(self):
        self.assert_col_types('random_numbers', ['double precision'] * 5)
            
if __name__ == '__main__':
    unittest.main()