''' Integration tests for PostgreSQL '''

import pgreaper
from pgreaper.postgres.loader import _modify_tables
from pgreaper.postgres import *
from pgreaper.core import ColumnList
from pgreaper.testing import *

import re
           
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
        pgreaper.copy_table(self.table, dbname=TEST_DB)
        
class IntsTest(PostgresTestCase):
    ''' Load lists of the first 100 integers '''
    
    drop_tables = ['ints']
    
    @classmethod
    def setUpClass(cls):           
        # Load the CSV file
        pgreaper.copy_csv(
            path.join(FAKE_CSV_DATA, 'ints.csv'),
            dbname=TEST_DB, name='ints',
            delimiter=',', header=0)
    
    def test_header(self):
        # Make sure header was read correctly
        schema_query = "SELECT \
                column_name, data_type \
            FROM \
                INFORMATION_SCHEMA.COLUMNS \
            WHERE table_name = 'ints'"
        
        self.cursor.execute(schema_query)
        
        headers = self.cursor.fetchall()
        correct = [
            ('a', 'bigint'),
            ('b', 'bigint'),
            ('c', 'bigint'),
            ('d', 'bigint'),
            ('e', 'bigint'),
            ('f', 'bigint'),
            ('g', 'bigint'),
            ('h', 'bigint'),
            ('i', 'bigint'),
            ('j', 'bigint'),
        ]
        
        self.assertEqual(headers, correct)
            
    def test_count(self):
        self.assertCount('ints', 100)
        
class IntsSkiplinesTest(PostgresTestCase):
    ''' Make sure the skip_lines argument works '''
    
    drop_tables = ['ints2']
    
    @classmethod
    def setUpClass(cls):           
        # Load the CSV file
        pgreaper.copy_csv(
            path.join(FAKE_CSV_DATA, 'ints_skipline.csv'),
            dbname=TEST_DB,
            name='ints2',
            delimiter=',',
            null_values='NA',
            header=0,
            skip_lines=1)
        
    def test_count(self):
        self.assertCount('ints2', 100)
        
# Need to reimplement
# class NullTest(PostgresTestCase):
    # ''' Using purchases.csv, make sure the null_values argument works '''
    
    # drop_tables = ['purchases']
    
    # @classmethod
    # def setUpClass(cls):           
        # # Load the CSV file
        # pgreaper.copy_csv(path.join(DATA_DIR, 'purchases.csv'),
            # dbname=TEST_DB,
            # name='purchases',
            # delimiter=',',
            # null_values='NA',
            # header=0)
            
    # def test_content(self):
        # # Make sure contents were loaded correctly
        # self.cursor.execute("SELECT * FROM purchases")
        # correct = [('Apples', '5'),
                   # ('Oranges', None),
                   # ('Guns', '666'),
                   # ('Butter', None)]
        # self.assertEqual(self.cursor.fetchall(), correct)

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
        pgreaper.copy_table(data, dbname=TEST_DB)
        
    def test_count(self):
        self.assertCount('countries_composite', 3)
        
# Need to reimplement
# class SubsetTest(PostgresTestCase):
    # ''' Test uploading a subset of columns '''
    
    # drop_tables = ['persons']
    
    # @classmethod
    # def setUpClass(cls):
        # data = path.join(MIMESIS_CSV_DATA, 'persons.csv')
        # pgreaper.copy_csv(data,
            # name='persons',
            # subset=['Full Name', 'Age', 'Email'],
            # dbname=TEST_DB)
        
    # def test_col_names(self):
        # self.assertColumnNames('persons', ['full_name', 'age', 'email'])
        
    # def test_col_types(self):
        # self.assert_col_types('persons', ['text', 'bigint', 'text'])
        
    # def test_count(self):
        # self.assertCount('persons', 50000)
        
if __name__ == '__main__':
    unittest.main()