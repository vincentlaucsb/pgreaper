''' Integration tests for JSON uploading '''

import pgreaper
from pgreaper.core import ColumnList
from pgreaper.testing import *

from pgreaper.postgres import *
from pgreaper.postgres.loader import _modify_tables

class PersonsTest(PostgresTestCase):
    ''' Test loading a JSON with outer-level flattening '''
    
    drop_tables = ['persons']
    
    @classmethod
    def setUpClass(cls):
        pgreaper.copy_json(
            os.path.join(JSON_DATA, 'persons.json'),
            name='persons',
            flatten='outer',
            dbname=TEST_DB,
        )
    
    def test_count(self):
        self.assertCount('persons', 50000)
        
    def test_schema(self):
        self.assertColumnContains('persons',
            ['full_name', 'age', 'email', 'telephone', 'nationality', 'occupation'])
            
    def test_col_types(self):
        # Like test above but also makes sure types are correct
        schema = get_table_schema('persons', conn=self.conn)
        
        correct_types = {
            'full_name': 'text',
            'email': 'text',
            'telephone': 'text',
            'nationality': 'text',
            'occupation': 'text',
            'age': 'numeric'
        }
        
        for cname, ctype in schema.as_tuples():
            self.assertEqual(ctype, correct_types[cname])

# class PersonsFilterTest(PostgresTestCase):
    # ''' Test subsetting a JSON '''
    
    # # drop_tables = ['persons_subset']
    
    # @classmethod
    # def setUpClass(cls):
        # pgreaper.copy_json(
            # os.path.join(MIMESIS_DIR, 'persons_nested_100k.json.gz'),
            # name='persons_subset',
            # compression='gzip',
            # filter=['full_name', "['personal']['age']",
                # "['contact']['email']"],
            # dbname=TEST_DB)
        
    # def test_count(self):
        # # Check correct number of rows loaded
        # self.assertEqual(pgreaper.read_pg(
            # 'SELECT count(*) FROM persons_subset',
            # dbname=TEST_DB
        # )['count'][0], 100000)
        
if __name__ == '__main__':
    unittest.main()