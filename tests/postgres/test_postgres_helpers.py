''' Helper Class and Function Tests '''
import pgreaper
from pgreaper.core import ColumnList
from pgreaper.testing import *

from pgreaper.postgres import *
from pgreaper.postgres.loader import _modify_tables

from os import path
import copy
import re
import unittest
import psycopg2

class HelpersTest(unittest.TestCase):
    ''' Tests of helper classes and functions '''
    
    def test_assert_table(self):
        x = 'harambe'
        with self.assertRaises(TypeError):
            pgreaper.table_to_pg(x, database='harambe')

class DBPostgresTest(PostgresTestCase):
    ''' Test if pgreaper.postgres.database functions work correctly '''
    
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
        
        with open(path.join(TEST_DIR, 'sql_queries',
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
        
if __name__ == '__main__':
    unittest.main()