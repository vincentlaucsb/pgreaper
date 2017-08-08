''' Tests for various pandas related functionalities '''

from sqlify.testing import *
from sqlify.postgres import get_schema
from sqlify.postgres.conn import postgres_connect
import sqlify

import pandas
import psycopg2
import unittest

class FromPandas(PostgresTestCase):
    ''' Test that pandas DataFrames are being read correctly '''
        
    drop_tables = ['chp_salaries']
        
    @classmethod
    def setUpClass(cls):
        ca_state_emp = pandas.read_csv(
            'data/2015_StateDepartment.zip', compression='zip')
        cls.chp = ca_state_emp[ca_state_emp['Entity Name'] == \
            'Highway Patrol, California']
        cls.table = sqlify.pandas_to_table(cls.chp)

        # Load to Postgres
        sqlify.pandas_to_pg(cls.chp, 
            name='chp_salaries', database='sqlify_pg_test')
        
    def test_to_table(self):
        ''' Test that DataFrames are converted to Table objects successfully '''
        
        # The correct values were manually inspected earlier
        self.assertEqual(FromPandas.table.col_types,
            ['INTEGER', 'TEXT', 'REAL', 'TEXT', 'TEXT', 'TEXT', 'BOOLEAN',
             'BOOLEAN', 'TEXT', 'REAL', 'REAL', 'REAL', 'REAL', 'REAL',
             'REAL', 'REAL', 'REAL', 'REAL', 'REAL', 'INTEGER', 'REAL',
             'REAL', 'TEXT' ,'TEXT', 'REAL', 'TEXT', 'REAL', 'REAL'])
            
    def test_to_pg(self):
        '''
        Test that DataFrames are uploaded to Postgres succesfully
         * Test if table name is correct
         * Spot check that column names are correct
        '''
        
        schema = get_schema(database='sqlify_pg_test')
        
        # Check table name
        self.assertIn('chp_salaries', schema['Table Name'])
        
        # Spot check
        for col_name in ['year', 'entity_type', 'position']:
            self.assertIn(col_name, schema['Column Name'])
            
    def test_pg_data_integrity(self):
        ''' Do some basic data integrity checks '''
        self.cursor.execute('SELECT count(*) FROM chp_salaries')
        
        num_rows = self.cursor.fetchall()[0][0]
        self.assertEqual(num_rows, 11248)

if __name__ == '__main__':
    unittest.main()