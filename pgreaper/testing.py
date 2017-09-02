''' Utility Functions for Tests '''

from pgreaper import Table
from pgreaper._globals import import_package, SQLIFY_PATH
from pgreaper.config import PG_DEFAULTS

from os import path
import copy
import unittest
import psycopg2
import os

TEST_DIR = os.path.join(os.path.split(SQLIFY_PATH)[:-1][0], 'tests')
DATA_DIR = os.path.join(TEST_DIR, 'data')
MIMESIS_DIR = os.path.join(TEST_DIR, 'mimesis')
REAL_DATA_DIR = os.path.join(TEST_DIR, 'real_data')

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
            self.conn = psycopg2.connect(**PG_DEFAULTS(dbname='pgreaper_pg_test'))
        except psycopg2.OperationalError:
            ''' Test database doesn't exist --> Create it '''
            with psycopg2.connect(**PG_DEFAULTS) as conn:
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                conn.cursor().execute('CREATE DATABASE pgreaper_pg_test')
            self.conn = psycopg2.connect(**PG_DEFAULTS(dbname='pgreaper_pg_test'))
        
        self.cursor = self.conn.cursor()
        
    def tearDown(self):
        self.conn.close()

    @classmethod
    def tearDownClass(cls):
        with psycopg2.connect(**PG_DEFAULTS(dbname='pgreaper_pg_test')) as conn:
            cursor = conn.cursor()
            for t in cls.drop_tables:
                cursor.execute('DROP TABLE IF EXISTS {}'.format(t))
            conn.commit()

def world_countries_cols():
    return ['Capital', 'Country', 'Currency', 'Demonym', 'Population']

# def world_countries_types():
    # return ['text', 'text', 'text', 'text', 'integer']
    
def world_countries():
    return [["Washington", "USA", "USD", 'American', 324774000],
            ["Moscow", "Russia", "RUB", 'Russian', 144554993],
            ["Ottawa", "Canada", "CAD", 'Canadian', 35151728]]
            
def world_countries_table():
    return Table('Countries',
        col_names = world_countries_cols(),
        row_values = world_countries()
    )