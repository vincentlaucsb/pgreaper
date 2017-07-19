''' Integration tests for PostgreSQL '''

import sqlify
from sqlify.postgres.conn import postgres_connect

import re
import unittest
import psycopg2

class StatesTest(unittest.TestCase):
    ''' Load a list of US states into the database '''
    
    @classmethod
    def setUpClass(cls):           
        # Load the CSV file
        sqlify.csv_to_pg('data/us_states.csv',
            database='sqlify_pg_test',
            name='us_states',
            delimiter=',',
            header=0)
    
        # Create a connection to database using default parameters
        cls.conn = postgres_connect(database='sqlify_pg_test')
        cls.cur = cls.conn.cursor()
        
    def test_header(self):
        # Make sure header was read correctly
        schema_query = "SELECT \
                column_name, data_type \
            FROM \
                INFORMATION_SCHEMA.COLUMNS \
            WHERE table_name = 'us_states'"
                
        StatesTest.cur.execute(schema_query)
        
        headers = StatesTest.cur.fetchall()
        correct = [('state', 'text'), ('abbreviation', 'text')]
        
        self.assertEqual(headers, correct)
            
    def test_content(self):
        # Make sure contents were loaded correctly
        states_query = "SELECT count(state) FROM us_states"
        StatesTest.cur.execute(states_query)
        
        # Should be 51 (all 50 states + DC)
        state_count = StatesTest.cur.fetchall()
        
        self.assertEqual(state_count, [(51,)])
        
    @classmethod
    def tearDownClass(cls):
        ''' Drop table when done
            TODO: Drop the database as well
        '''
        
        cls.cur.execute('DROP TABLE IF EXISTS us_states')
        cls.conn.commit()
        
class NullTest(unittest.TestCase):
    ''' Using purchases.csv, make sure the null_values argument works '''
    
    @classmethod
    def setUpClass(cls):           
        # Load the CSV file
        sqlify.csv_to_pg('data/purchases.csv',
            database='sqlify_pg_test',
            name='purchases',
            delimiter=',',
            null_values='NA',
            header=0)
    
        # Create a connection to database using default parameters
        cls.conn = postgres_connect(database='sqlify_pg_test')
        cls.cur = cls.conn.cursor()
        
    def test_content(self):
        # Make sure contents were loaded correctly
        NullTest.cur.execute("SELECT * FROM purchases")
        
        correct = [('Apples', '5'),
                   ('Oranges', None),
                   ('Guns', '666'),
                   ('Butter', None)]
        self.assertEqual(NullTest.cur.fetchall(), correct)
        
    @classmethod
    def tearDownClass(cls):
        ''' Drop table when done '''
        
        cls.cur.execute('DROP TABLE IF EXISTS purchases')
        cls.conn.commit()
        
class SkipLinesTest(unittest.TestCase):
    ''' Using purchases2.csv, make sure the skip_lines argument works '''
    
    @classmethod
    def setUpClass(cls):           
        # Load the CSV file
        sqlify.csv_to_pg('data/purchases2.csv',
            database='sqlify_pg_test',
            name='purchases2',
            delimiter=',',
            null_values='NA',
            header=0,
            skip_lines=2)
    
        # Create a connection to database using default parameters
        cls.conn = postgres_connect(database='sqlify_pg_test')
        cls.cur = cls.conn.cursor()
        
    def test_content(self):
        # Make sure contents were loaded correctly
        SkipLinesTest.cur.execute("SELECT count(product) FROM purchases2")
        
        correct = [(4,)]
        self.assertEqual(SkipLinesTest.cur.fetchall(), correct)
        
    @classmethod
    def tearDownClass(cls):
        ''' Drop table when done '''
        
        cls.cur.execute('DROP TABLE IF EXISTS purchases2')
        cls.conn.commit()

class TransformTest(unittest.TestCase):
    ''' Make sure transformations work. Here we test whitespace stripping '''
    
    @classmethod
    def setUpClass(cls):           
        # Load the TEXT file
        sqlify.text_to_pg('data/countries-bad-spacing.txt',
            database='sqlify_pg_test',
            name='countries',
            delimiter='\t',
            header=0,
            transform={
                'all': lambda x: re.sub('^(?=) *|(?=) *$', '', x),
            })
    
        # Create a connection to database using default parameters
        cls.conn = postgres_connect(database='sqlify_pg_test')
        cls.cur = cls.conn.cursor()
    
    def test_content(self):
        # Make sure contents were loaded correctly
        TransformTest.cur.execute("SELECT * FROM countries")
        
        correct  = [("Washington", "USA", "USD", 'American', 324774000),
                    ("Moscow", "Russia", "RUB", 'Russian', 144554993),
                    ("Ottawa", "Canada", "CAD", 'Canadian', 35151728)]
                      
        self.assertEqual(TransformTest.cur.fetchall(), correct)
        
    @classmethod
    def tearDownClass(cls):
        ''' Drop table when done '''
        
        cls.cur.execute('DROP TABLE IF EXISTS countries')
        cls.conn.commit()    
        
if __name__ == '__main__':
    unittest.main()