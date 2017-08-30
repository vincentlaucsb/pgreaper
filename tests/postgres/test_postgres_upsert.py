''' Integration tests for PostgreSQL '''

import pgreaper
from pgreaper.core import ColumnList
from pgreaper.testing import *

from pgreaper.postgres import *
from pgreaper.postgres.conn import postgres_connect
from pgreaper.postgres.loader import _modify_tables

from os import path
import unittest
import psycopg2

class ModifyTest(unittest.TestCase):
    ''' Makes sure that _modify_table() performs the correct alterations '''
    
    def test_expand_table(self, expand_input=True):
        # Input table has several less columns than mock SQL table
        input = world_countries_table().subset('Capital', 'Country')
        sql_table = world_countries_table()
        
        new_table = _modify_tables(
            input,
            ColumnList(sql_table.col_names, sql_table.col_types),
            expand_input=expand_input)
            
        # Case insensitive comparison
        new_cols = ColumnList(new_table.col_names, new_table.col_types)
        sql_cols = ColumnList(sql_table.col_names, sql_table.col_types)
        self.assertEqual(new_cols, sql_cols)
        
    #@unittest.skip('Debugging')
    def test_no_expand_table(self):
        '''
        Make sure input is not changed unless explicitly specified
        via the expand_input parameter
        '''
        
        with self.assertRaises(ValueError):
            self.test_expand_table(expand_input=False)

class UpsertTest(PostgresTestCase):
    ''' Test various UPSERT options '''
    
    data = pgreaper.Table('Countries', col_names = world_countries_cols(), row_values = world_countries())
        # p_key = 1,

    data.append(["Beijing", "China", "CNY", 'Chinese', 1373541278])
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
    
    #@unittest.skip('Debugging')
    def test_on_conflict_do_nothing(self):
        pgreaper.table_to_pg(self.data[0: 2],
            name='countries',
            dbname='pgreaper_pg_test')
        
        assert(self.data[0: 2].p_key == 1)
        
        pgreaper.table_to_pg(self.data[2: ],
            name='countries',
            dbname='pgreaper_pg_test',
            on_p_key='nothing')
        
        self.cursor.execute('SELECT count(*) FROM countries')
        self.assertEqual(self.cursor.fetchall()[0][0], 4)

    #@unittest.skip('Debugging')
    def test_insert_or_replace(self):
        pgreaper.table_to_pg(UpsertTest.data,
            name='countries',
            dbname='pgreaper_pg_test')
        
        # Set entire population column to 0        
        self.data.apply('Population', lambda x: 0)
        
        pgreaper.table_to_pg(self.data,
            on_p_key='replace',
            name='countries',
            dbname='pgreaper_pg_test')
        
        self.cursor.execute('SELECT sum(population::bigint) FROM countries')
        self.assertEqual(self.cursor.fetchall()[0][0], 0)    
        
    #@unittest.skip('Debugging')
    def test_reorder_do_nothing(self):
        ''' Test if SQLify can reorder input to match SQL table '''
        
        # Load Table
        pgreaper.table_to_pg(UpsertTest.data[0: 2],
            name='countries',
            dbname='pgreaper_pg_test')

        # Make Table with same columns, different order then try to load
        data = self.data[2: ].reorder(4, 3, 2, 0, 1)
        pgreaper.table_to_pg(data, name='countries',
            dbname='pgreaper_pg_test',
            reorder=True,
            on_p_key='nothing')
            
        self.cursor.execute('SELECT count(*) FROM countries')           
        self.assertEqual(self.cursor.fetchall()[0][0], 4)
        
    #@unittest.skip('Debugging')
    def test_append(self):
        ''' Regular append, no primary key constraint '''
        self.data.p_key = None

        pgreaper.table_to_pg(self.data,
            name='countries', dbname='pgreaper_pg_test')
        pgreaper.table_to_pg(self.data,
            name='countries', dbname='pgreaper_pg_test', append=True)
            
        self.cursor.execute('SELECT count(*) FROM countries')
        self.assertEqual(self.cursor.fetchall()[0][0], 8)
        
    def test_expand_input(self, expand_input=True):
        ''' Test that input expansion is handled properly '''
        pgreaper.table_to_pg(self.data[0:2],
            name='countries', dbname='pgreaper_pg_test')
        
        # Needs to line up columns correctly with existing schema
        needs_expanding = self.data[2: ].subset('Country', 'Population')
        
        pgreaper.table_to_pg(needs_expanding,
            name='countries',
            dbname='pgreaper_pg_test',
            expand_input=expand_input)
            
        # Check that expanded columns were filled with NULLs
        self.cursor.execute('SELECT count(*) FROM countries WHERE'
            ' capital is NULL')
        count = self.cursor.fetchall()[0][0]
        self.assertEqual(count, 2)
        
    def test_no_expand_input(self):
        ''' Test that input expansion doesn't happen unless explicitly specified '''
        
        with self.assertRaises(ValueError):
            self.test_expand_input(expand_input=False)
   
    def test_expand_sql(self, expand_sql=True):
        ''' Test that adding columns to SQL tables works '''
        data = copy.deepcopy(self.data)
        data.delete('Population')
        
        # Upload truncated data set
        pgreaper.table_to_pg(data[0: 2], name='countries', dbname='pgreaper_pg_test')
        
        pgreaper.table_to_pg(self.data[2: ], name='countries',
            dbname='pgreaper_pg_test', expand_sql=expand_sql)
            
        # Test that extra columns were added
        self.cursor.execute('SELECT count(population) FROM countries '
        'WHERE population IS NOT NULL')
        self.assertEqual(self.cursor.fetchall()[0][0], 2)
        
    def test_no_expand_sql(self):
        '''
        Test that adding columns to SQL tables
        doesn't happen unless explicitly specified
        '''
        
        with self.assertRaises(ValueError):
            self.test_expand_sql(expand_sql=False)
            
if __name__ == '__main__':
    unittest.main()