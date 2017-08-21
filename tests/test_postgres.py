''' Integration tests for PostgreSQL '''

import sqlify
from sqlify.core import ColumnList
from sqlify.testing import *

from sqlify.postgres import *
from sqlify.postgres.conn import postgres_connect
from sqlify.postgres.loader import _modify_tables

from os import path
import copy
import re
import unittest
import psycopg2

'''
Helper Class and Function Tests
================================
'''

class HelpersTest(unittest.TestCase):
    ''' Tests of helper classes and functions '''
    
    def test_assert_table(self):
        x = 'harambe'
        with self.assertRaises(TypeError):
            sqlify.table_to_pg(x, database='harambe')

class DBPostgresTest(PostgresTestCase):
    ''' Test if sqlify.postgres.database functions work correctly '''
    
    drop_tables = ['countries_test']
    
    def test_create_table_pkey(self):
        ''' Test that generate a CREATE TABLE statement includes the primary key '''
        table = world_countries_table()[0: 2]
        table.p_key = 'Country'
        
        self.assertEqual(create_table(table).lower(),
            'CREATE TABLE IF NOT EXISTS Countries (Capital TEXT, Country '
            'TEXT PRIMARY KEY, Currency TEXT, Demonym TEXT, Population TEXT)'.lower())
            
    def test_get_pkey(self):
        ''' Test if getting list of primary keys is accurate '''
        # Load USA, Russia, and Canada data into 'countries_test'
        
        with open(path.join('sql_queries',
            'countries_test.sql'), 'r') as countries:
            countries = ''.join(countries.readlines())            
            self.cursor.execute(countries)
            self.conn.commit()
        
        self.assertEqual(
            get_primary_keys('countries_test', conn=self.conn),
            set(['USA', 'Canada', 'Russia']))
            
    def test_get_schema(self):
        schema = get_table_schema('countries_test', conn=self.conn)
        self.assertEqual(schema.col_names,
            ['capital', 'country','population'])
            
    def test_get_schema_dne(self):
        schema = get_table_schema('sasquatch', conn=self.conn)
        self.assertEqual(schema, ColumnList())
            
'''
Uploading Tests
=================
'''

class MalformedTest(PostgresTestCase):
    '''
    Integration test
    
     * Make sure loading Tables crreated in Python works
     * Test that names are cleaned
    '''
    
    drop_tables = ['countries']
    
    def setUp(self):
        super(MalformedTest, self).setUp()
        self.table = world_countries_table()
        
        def name_ruinator(text):
            ''' Makes table and column names SQL unsafe '''
            return '/{}; ***'.format(text)
        
        self.table.col_names = [name_ruinator(i) for i in world_countries_cols()]
        
    def test_load(self):
        sqlify.table_to_pg(self.table, dbname='sqlify_pg_test')
        
class StatesTest(PostgresTestCase):
    ''' Load a list of US states into the dbname '''
    
    drop_tables = ['us_states']
    
    @classmethod
    def setUpClass(cls):           
        # Load the CSV file
        sqlify.csv_to_pg('data/us_states.csv', dbname='sqlify_pg_test', name='us_states', delimiter=',', header=0)
    
    def test_header(self):
        # Make sure header was read correctly
        schema_query = "SELECT \
                column_name, data_type \
            FROM \
                INFORMATION_SCHEMA.COLUMNS \
            WHERE table_name = 'us_states'"
        
        self.cursor.execute(schema_query)
        
        headers = self.cursor.fetchall()
        correct = [('state', 'text'), ('abbreviation', 'text')]
        
        self.assertEqual(headers, correct)
            
    #@unittest.skip('Debugging')
    def test_content(self):
        # Make sure contents were loaded correctly
        states_query = "SELECT count(state) FROM us_states"
        self.cursor.execute(states_query)
        
        # Should be 51 (all 50 states + DC)
        state_count = self.cursor.fetchall()
        self.assertEqual(state_count, [(51,)])
        
class NullTest(PostgresTestCase):
    ''' Using purchases.csv, make sure the null_values argument works '''
    
    drop_tables = ['purchases']
    
    @classmethod
    def setUpClass(cls):           
        # Load the CSV file
        sqlify.csv_to_pg('data/purchases.csv',
            dbname='sqlify_pg_test',
            name='purchases',
            delimiter=',',
            null_values='NA',
            header=0)
            
    #@unittest.skip('Debugging')
    def test_content(self):
        # Make sure contents were loaded correctly
        self.cursor.execute("SELECT * FROM purchases")
        correct = [('Apples', '5'),
                   ('Oranges', None),
                   ('Guns', '666'),
                   ('Butter', None)]
        self.assertEqual(self.cursor.fetchall(), correct)
            
class SkipLinesTest(PostgresTestCase):
    ''' Using purchases2.csv, make sure the skip_lines argument works '''
    
    drop_tables = ['purchases2']
    
    @classmethod
    def setUpClass(cls):           
        # Load the CSV file
        sqlify.csv_to_pg('data/purchases2.csv',
            dbname='sqlify_pg_test',
            name='purchases2',
            delimiter=',',
            null_values='NA',
            header=0,
            skip_lines=1)
        
    #@unittest.skip('Debugging')
    def test_content(self):
        # Make sure contents were loaded correctly
        self.cursor.execute("SELECT count(product) FROM purchases2")
        correct = [(4,)]
        self.assertEqual(self.cursor.fetchall(), correct)

class CompositePKeyTest(PostgresTestCase):
    ''' Test uploading a table with a composite primary key '''
    # NOTE: This might not be strong enough
    
    drop_tables = ['countries_composite']

    @classmethod
    def setUpClass(cls):
        data = world_countries_table()
        data.name = 'countries_composite'
        data.add_col('Year', 2017)
        p_key = ('Country', 'Year')
        sqlify.table_to_pg(data, dbname='sqlify_pg_test')
        
    def test_contents(self):
        ''' Do some basic integrity checks '''
        self.cursor.execute("SELECT count(year) FROM countries_composite")
        correct = [(3,)]
        self.assertEqual(self.cursor.fetchall(), correct)
       
class ZipTest(PostgresTestCase):
    ''' Test uploading a compressed file '''
    
    drop_tables = ['ca_employees_zip']
    
    @classmethod
    def setUpClass(self):
        zip_file = sqlify.read_zip(os.path.join(
            'data', '2015_StateDepartment.zip'))
        
        sqlify.csv_to_pg(zip_file[0],
            name='ca_employees_zip',
            dbname='sqlify_pg_test')
            
    def test_integrity(self):
        ''' Do some basic integrity checks '''
        pass
        
if __name__ == '__main__':
    unittest.main()