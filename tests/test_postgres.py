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

class DBPostgresTest(PostgresTestCase):
    ''' Test if sqlify.postgres.database functions work correctly '''
    
    drop_tables = ['countries_test']
    
    def test_create_table_pkey(self):
        ''' Test that generate a CREATE TABLE statement includes the primary key '''
        table = world_countries_table()[0: 2]
        table.p_key = 'Country'
        
        self.assertEqual(create_table(table),
            'CREATE TABLE IF NOT EXISTS Countries (Capital TEXT, Country '
            'TEXT PRIMARY KEY, Currency TEXT, Demonym TEXT, Population TEXT)'
        )
            
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

class ModifyTest(unittest.TestCase):
    ''' Makes sure that _modify_table() performs the correct alterations '''
    
    def test_expand_table(self):
        # Input table has several less columns than mock SQL table
        input = world_countries_table().subset('Capital', 'Country')
        sql_table = world_countries_table()
        
        new_table = _modify_tables(
            input,
            ColumnList(sql_table.col_names, sql_table.col_types))
            
        # Case insensitive comparison
        new_cols = ColumnList(new_table.col_names, new_table.col_types)
        sql_cols = ColumnList(sql_table.col_names, sql_table.col_types)
        self.assertEqual(new_cols, sql_cols)
        
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
            skip_lines=2)
        
    def test_content(self):
        # Make sure contents were loaded correctly
        self.cursor.execute("SELECT count(product) FROM purchases2")
        correct = [(4,)]
        self.assertEqual(self.cursor.fetchall(), correct)

# class TransformTest(PostgresTestCase):
    # ''' Make sure transformations work. Here we test whitespace stripping '''
    
    # @classmethod
    # def setUpClass(cls):           
        # # Load the TEXT file
        # sqlify.text_to_pg('data/countries-bad-spacing.txt',
            # dbname='sqlify_pg_test',
            # name='countries',
            # delimiter='\t',
            # header=0,
            # transform={
                # 'all': lambda x: re.sub('^(?=) *|(?=) *$', '', x),
            # })
    
        # # Create a connection to dbname using default parameters
        # cls.conn = postgres_connect(dbname='sqlify_pg_test')
        # cls.cur = cls.conn.cursor()
    
    # def test_content(self):
        # # Make sure contents were loaded correctly
        # TransformTest.cur.execute("SELECT * FROM countries")

        # correct  = [("Washington", "USA", "USD", 'American', 324774000),
                    # ("Moscow", "Russia", "RUB", 'Russian', 144554993),
                    # ("Ottawa", "Canada", "CAD", 'Canadian', 35151728)]
                      
        # self.assertEqual(TransformTest.cur.fetchall(), correct)
        
    # @classmethod
    # def tearDownClass(cls):
        # ''' Drop table when done '''
        
        # cls.cur.execute('DROP TABLE IF EXISTS countries')
        # cls.conn.commit()
        
class UpsertTest(PostgresTestCase):
    ''' Test various UPSERT options '''
    
    data = sqlify.Table('Countries',
        # p_key = 1,
        col_names = world_countries_cols(),
        row_values = world_countries())
    data.append(["Beijing", "China", "CNY", 'Chinese', "1373541278"])
    data.p_key = 'Country'
    drop_tables = ['countries']
    
    def setUp(self):
        super(UpsertTest, self).setUp()
        
        # DROP TABLE IF EXISTS causes psycopg2 to hang...
        try:
            self.cursor.execute('DROP TABLE countries')
            self.conn.commit()
        except psycopg2.ProgrammingError:
            self.conn.rollback()
    
    def test_on_conflict_do_nothing(self):
        sqlify.table_to_pg(self.data[0: 2],
            name='countries',
            dbname='sqlify_pg_test')
        
        assert(self.data[0: 2].p_key == 1)
        
        sqlify.table_to_pg(self.data[2: ],
            name='countries',
            dbname='sqlify_pg_test',
            on_p_key='nothing')
        
        self.cursor.execute('SELECT count(*) FROM countries')
        self.assertEqual(self.cursor.fetchall()[0][0], 4)

    def test_insert_or_replace(self):
        sqlify.table_to_pg(UpsertTest.data,
            name='countries',
            dbname='sqlify_pg_test')
        
        # Set entire population column to 0        
        self.data.apply('Population', lambda x: 0)
        
        sqlify.table_to_pg(self.data,
            on_p_key='replace',
            name='countries',
            dbname='sqlify_pg_test')
        
        self.cursor.execute('SELECT sum(population::bigint) FROM countries')
        self.assertEqual(self.cursor.fetchall()[0][0], 0)    
        
    def test_reorder_do_nothing(self):
        ''' Test if SQLify can reorder input to match SQL table '''
        
        # Load Table
        sqlify.table_to_pg(UpsertTest.data[0: 2],
            name='countries',
            dbname='sqlify_pg_test')

        # Make Table with same columns, different order then try to load
        data = self.data[2: ].reorder(4, 3, 2, 0, 1)
        sqlify.table_to_pg(data, name='countries',
            dbname='sqlify_pg_test',
            reorder=True,
            on_p_key='nothing')
            
        self.cursor.execute('SELECT count(*) FROM countries')           
        self.assertEqual(self.cursor.fetchall()[0][0], 4)
        
    def test_append(self):
        ''' Regular append, no primary key constraint '''
        self.data.p_key = None
        
        sqlify.table_to_pg(self.data,
            name='countries', dbname='sqlify_pg_test')
        sqlify.table_to_pg(self.data,
            name='countries', dbname='sqlify_pg_test', append=True)
            
        self.cursor.execute('SELECT count(*) FROM countries')
        self.assertEqual(self.cursor.fetchall()[0][0], 8)
        
    def test_expand_input(self):
        ''' Test that input expansion is handled properly '''
        sqlify.table_to_pg(self.data[0:2],
            name='countries', dbname='sqlify_pg_test')
        
        needs_expanding = self.data[2: ].subset('Country', 'Population')
        sqlify.table_to_pg(needs_expanding,
            name='countries', dbname='sqlify_pg_test')
            
        # Check that expanded columns were filled with NULLs
        self.cursor.execute('SELECT count(*) FROM countries WHERE'
            ' capital is NULL', 2)
        
if __name__ == '__main__':
    unittest.main()