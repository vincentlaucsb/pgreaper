#################
# SQLType Tests #
#################

from pgreaper._globals import PG_KEYWORDS
from pgreaper.testing import *
from pgreaper.core import ColumnList
from pgreaper.core.schema import SQLType
import pgreaper

from functools import partial
import unittest

class AddTest(unittest.TestCase):
    '''
    Test that "adding" SQLTypes produces the correct type, i.e.
    the type that makes them both compatible in the same column
    '''
    
    def setUp(self):
        self.table = pgreaper.Table(name='Test', dialect='postgres')
        
    def testMixedNumbers(self):
        ''' Test that bigint + dp = dp '''
        self.assertEqual(
            SQLType(int, self.table) + SQLType(float, self.table),
            'double precision')
            
    def testType1(self):
        '''
        Test that you can add SQLTypes to Python type objects
        and get a meaningful result
        '''
        self.assertEqual(
            SQLType(int, self.table) + type(3.14), 'double precision')
            
    def testType2(self):
        self.assertEqual(
            SQLType(dict, self.table) + type([]), 'jsonb')
    
class SQLiteTypeCounterTest(unittest.TestCase):
    ''' Test if type counter works '''
    
    def test_simple_case(self):
        tbl = world_countries_table()
        tbl.dialect = 'sqlite'
        tbl.guess_type()
        
        self.assertEqual(
            tbl.col_types,
            ['text', 'text', 'text', 'text', 'integer'])
                        
    def test_mixed_case(self):
        col_names = ['mixed_data', 'mixed_numbers', 'just_int']
        row_values = [['abcd', 123, 123],
                      ['bcda', 1.23, 222],
                      [1111, 3.14, 222]]
        
        tbl = Table('Some Data', dialect='sqlite', col_names=col_names,
            row_values=row_values)
        tbl.guess_type()
        
        expected_col_types = ['text', 'real', 'integer']
        self.assertEqual(tbl.col_types, expected_col_types)
        
if __name__ == '__main__':
    unittest.main()