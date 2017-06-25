''' Tests regarding converting SQLite databases to Postgres databases '''

import sqlify
from sqlify.postgres.conn import postgres_connect
from sqlify.sqlite import to_postgres

import os
import unittest
import sqlite3

class HelpersTest(unittest.TestCase):
    ''' Test helper functions such as get_schema() '''
    
    def test_get_schema(self):
        correct_schema = {
            'col_names': ['UNIFORM0_1', 'UNIFORM0_10', 'STDNORM',
                          'NORM5_25', 'EXP_1'],
            'col_types': ['REAL', 'REAL', 'REAL', 'REAL', 'REAL']
        }
        
        self.assertEqual(
            to_postgres.get_schema('data/sqlite_numbers.db', 'random_numbers'),
            correct_schema)
    
    def test_convert_schema(self):
        original_types = ['text', 'text', 'integer', 'real']
        correct_types = ['text', 'text', 'bigint', 'double precision']
        
        self.assertEqual(
            to_postgres.convert_schema(original_types),
            correct_types)
            
class SQLiteToPGTest(unittest.TestCase):
    ''' Test if SQLite to Postgres conversion works '''
    
    @classmethod
    def setUpClass(cls):
        ''' Convert a SQLite database of random numbers to Postgres '''
        
        sqlify.sqlite_to_postgres(
            sqlite_db='data/sqlite_numbers.db',
            pg_db='sqlify_pg_test',
            name='random_numbers'
        )
        
        # Create a connection to database using default parameters
        cls.conn = postgres_connect(database='sqlify_pg_test')
        cls.cur = cls.conn.cursor()
        
    def test_content(self):
        ''' Check to see if contents were uploaded successfully '''
        SQLiteToPGTest.cur.execute(
            "SELECT count(UNIFORM0_1) FROM random_numbers")
        
        correct = [(500,)]
        self.assertEqual(SQLiteToPGTest.cur.fetchall(), correct)
        
    @classmethod
    def tearDownClass(cls):
        ''' Drop table when done '''
        
        cls.cur.execute('DROP TABLE IF EXISTS random_numbers')
        cls.conn.commit()
            
if __name__ == '__main__':
    unittest.main()