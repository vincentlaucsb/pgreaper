''' Utility Functions for Tests '''

from pgreaper import Table, read_pg
from pgreaper.postgres import get_table_schema
from pgreaper._globals import import_package, SQLIFY_PATH
from pgreaper.config import PG_DEFAULTS

from os import path
import copy
import unittest
import psycopg2
import os

TEST_DIR = os.path.join(os.path.split(SQLIFY_PATH)[:-1][0], 'tests')
DATA_DIR = os.path.join(TEST_DIR, 'data')
CSV_DATA = os.path.join(TEST_DIR, 'csv-data')
JSON_DATA = os.path.join(TEST_DIR, 'json-data')
FAKE_CSV_DATA = os.path.join(CSV_DATA, 'fake_data')
MIMESIS_CSV_DATA = os.path.join(CSV_DATA, 'mimesis_data')
REAL_CSV_DATA = os.path.join(CSV_DATA, 'real_data')

TEST_DB = 'pgreaper_test'

# Flag for testing optional dependencies
if not import_package('pandas'):
    TEST_OPTIONAL_DEPENDENCY = False
else:
    TEST_OPTIONAL_DEPENDENCY = True

class PostgresTestCase(unittest.TestCase):
    '''
    Subclass of unittest.TestCase which:
     * Drops tables in class attrib drop_tables after all tests have run
     * Supplies each test_* method with a working connection
       * and closes it after execution
     * Provides each instance with a deepcopy of whatever is in class attrib data
    '''

    data = None
    drop_tables = []

    def setUp(self):
        if type(self).data:
            self.data = copy.deepcopy(type(self).data)
            
        try:
            self.conn = psycopg2.connect(**PG_DEFAULTS(dbname='pgreaper_test'))
        except psycopg2.OperationalError:
            ''' Test database doesn't exist --> Create it '''
            with psycopg2.connect(**PG_DEFAULTS) as conn:
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                conn.cursor().execute('CREATE DATABASE pgreaper_test')
            self.conn = psycopg2.connect(**PG_DEFAULTS(dbname='pgreaper_test'))
        
        self.cursor = self.conn.cursor()
        
    def tearDown(self):
        self.conn.close()

    @classmethod
    def tearDownClass(cls):
        with psycopg2.connect(**PG_DEFAULTS(dbname='pgreaper_test')) as conn:
            cursor = conn.cursor()
            for t in cls.drop_tables:
                cursor.execute('DROP TABLE IF EXISTS {}'.format(t))
            conn.commit()
            
    def assertColumnNames(self, table, col_names):
        ''' Assert that a table has the specified column names '''
        schema = get_table_schema(table, conn=self.conn)
        self.assertEqual(schema.col_names, col_names)
        
    def assertColumnTypes(self, table, col_types):
        ''' Assert that a table has the specified column types '''
        schema = get_table_schema(table, conn=self.conn)
        self.assertEqual(schema.col_types, col_types)
        
    def assertColumnContains(self, table, col_names):
        ''' Assert that a table has the column names in any order '''
        schema = get_table_schema(table, conn=self.conn)
        
        for col in col_names:
            self.assertIn(col, schema.col_names)
            
    def assertCount(self, table, n):
        ''' Assert that a table has n rows '''
        row_count = read_pg(
            'SELECT count(*) FROM {} as COUNT'.format(table),
            conn=self.conn)
        self.assertEqual(row_count['count'][0], n)

def world_countries_cols():
    return ['Capital', 'Country', 'Currency', 'Demonym', 'Population']

# def world_countries_types():
    # return ['text', 'text', 'text', 'text', 'bigint']
    
def world_countries():
    return [["Washington", "USA", "USD", 'American', 324774000],
            ["Moscow", "Russia", "RUB", 'Russian', 144554993],
            ["Ottawa", "Canada", "CAD", 'Canadian', 35151728]]
            
def world_countries_table():
    return Table('Countries',
        col_names = world_countries_cols(),
        row_values = world_countries()
    )