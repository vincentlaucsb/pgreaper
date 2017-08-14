''' Tests regarding converting SQLite databases to Postgres databases '''

import sqlify
from sqlify.testing import PostgresTestCase
from sqlify.postgres.conn import postgres_connect
from sqlify.sqlite import to_postgres

import os
import unittest
import sqlite3

# Need to rewrite this
# class HelpersTest(unittest.TestCase):
    # ''' Test helper functions such as get_schema() '''
    
    # def test_get_schema(self):
        # correct_schema = {
            # 'col_names': ['UNIFORM0_1', 'UNIFORM0_10', 'STDNORM',
                          # 'NORM5_25', 'EXP_1'],
            # 'col_types': ['REAL', 'REAL', 'REAL', 'REAL', 'REAL']
        # }
        
        # self.assertEqual(
            # to_postgres.get_schema('data/sqlite_numbers.db', 'random_numbers'),
            # correct_schema)
    
    # def test_convert_schema(self):
        # original_types = ['text', 'text', 'integer', 'real']
        # correct_types = ['text', 'text', 'bigint', 'double precision']
        
        # self.assertEqual(
            # to_postgres.convert_schema(original_types, from_='sqlite', to_='postgres'),
            # correct_types)
            
class SQLiteToPGTest(PostgresTestCase):
    ''' Test if SQLite to Postgres conversion works '''
    
    drop_tables = ['random_numbers']
    
    @classmethod
    def setUpClass(cls):
        ''' Convert a SQLite database of random numbers to Postgres '''
        sqlify.sqlite_to_postgres(
            sqlite_db='data/sqlite_numbers.db',
            dbname='sqlify_pg_test',
            name='random_numbers'
        )
        
    def test_content(self):
        ''' Check to see if contents were uploaded successfully '''
        self.cursor.execute("SELECT count(UNIFORM0_1) FROM random_numbers")
        
        correct = [(500,)]
        self.assertEqual(self.cursor.fetchall(), correct)
            
if __name__ == '__main__':
    unittest.main()