''' Tests for various pandas related functionalities '''

from pgreaper.testing import *
from pgreaper.postgres import get_schema, postgres_connect
import pgreaper

# pandas is an optional dependency
try:
    import pandas
except ImportError:
    pass
    
import psycopg2
import unittest

class FromPandas(PostgresTestCase):
    ''' Test that pandas DataFrames are being read correctly '''
        
    drop_tables = ['chp_salaries']
        
    @classmethod
    def setUpClass(cls):
        try:
            ca_state_emp = pandas.read_csv(
                os.path.join(REAL_DATA_DIR, '2015_StateDepartment.zip'),
                compression='zip')
            cls.chp = ca_state_emp[ca_state_emp['Entity Name'] == \
                'Highway Patrol, California']

            # Load to Postgres
            pgreaper.copy_df(cls.chp, name='chp_salaries', dbname=TEST_DB)
        except NameError:
            pass
            
    @unittest.skipUnless(TEST_OPTIONAL_DEPENDENCY, 'Skipping optional dependency')
    def test_to_pg(self):
        '''
        Test that DataFrames are uploaded to Postgres succesfully
         * Test if table name is correct
         * Spot check that column names are correct
        '''
        
        schema = get_schema(dbname=TEST_DB)
        
        # Check table name
        self.assertIn('chp_salaries', schema['Table Name'])
        
        # Spot check
        for col_name in ['year', 'entity_type', 'position']:
            self.assertIn(col_name, schema['Column Name'])
            
    @unittest.skipUnless(TEST_OPTIONAL_DEPENDENCY, 'Skipping optional dependency')
    def test_count(self):
        self.assertCount('chp_salaries', 11248)
        
    @classmethod
    def tearDownClass(cls):
        if TEST_OPTIONAL_DEPENDENCY:
            super(FromPandas, cls).tearDownClass()
        else:
            pass

if __name__ == '__main__':
    unittest.main()