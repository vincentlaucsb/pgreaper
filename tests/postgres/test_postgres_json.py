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
            os.path.join(MIMESIS_DIR, 'persons_nested_100k.json.gz'),
            name='persons',
            compression='gzip',
            # flatten='outer', --> This is the default setting
            dbname=TEST_DB,
        )
    
    def test_count(self):
        self.assertCount('persons', 100000)
        
    def test_schema(self):
        self.assertColumnContains('persons',
            ['full_name', 'contact', 'nationality', 'personal', 'occupation'])

class PersonsNestedTest(PostgresTestCase):
    ''' Test flattening and loading a JSON '''
    
    drop_tables = ['persons_nested']
    
    @classmethod
    def setUpClass(cls):
        pgreaper.copy_json(
            os.path.join(MIMESIS_DIR, 'persons_nested_100k.json.gz'),
            name='persons_nested',
            compression='gzip',
            flatten='all',
            dbname=TEST_DB,
        )
    
    def test_count(self):
        self.assertCount('persons_nested', 100000)
        
    def test_schema(self):
        self.assertColumnContains('persons_nested',
            ['personal_age', 'contact_email', 'contact_telephone_mobile',
             'contact_telephone_work'])
        
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