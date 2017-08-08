''' Utility Functions for Tests '''

from sqlify import Table
from sqlify._globals import Singleton
from sqlify.config import PG_DEFAULTS

import copy
import unittest
import psycopg2

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
            
        self.conn = psycopg2.connect(**PG_DEFAULTS(dbname='sqlify_pg_test'))
        self.cursor = self.conn.cursor()
        
    def tearDown(self):
        self.conn.close()

    @classmethod
    def tearDownClass(cls):
        with psycopg2.connect(**PG_DEFAULTS(dbname='sqlify_pg_test')) as conn:
            cursor = conn.cursor()
            for t in cls.drop_tables:
                cursor.execute('DROP TABLE IF EXISTS {}'.format(t))
            conn.commit()

def world_countries_cols():
    return ['Capital', 'Country', 'Currency', 'Demonym', 'Population']

def world_countries_types():
    return ['text', 'text', 'text', 'text', 'integer']
    
def world_countries():
    return [["Washington", "USA", "USD", 'American', "324774000"],
            ["Moscow", "Russia", "RUB", 'Russian', "144554993"],
            ["Ottawa", "Canada", "CAD", 'Canadian', "35151728"]]
            
def world_countries_table():
    return Table('Countries',
        col_names = world_countries_cols(),
        row_values = world_countries()
    )