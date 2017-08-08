''' Test if ColumnList helper class works '''

from sqlify.testing import *
from sqlify.core import ColumnList

from functools import partial
import unittest

class TestColumns(unittest.TestCase):
    ''' Test that this helper data structure works '''

    def test_lower(self):
        # Assert that ColumnLists always return lowercase versions of themselves
        columns1 = ColumnList(
            col_names = ['thanks', 'OBaMa'],
            col_types = ['text', 'text'])
        columns2 = ColumnList(
            col_names = ['thanks', 'obama'],
            col_types = ['text', 'text'])
        
        self.assertEqual(columns1, columns2)
        
    def test_simple_add(self):
        ''' Test that + preserves order of first summand '''
        
        columns1 = ColumnList(
            col_names = ['thanks', 'obama'],
            col_types = ['text', 'text'])
        columns2 = ColumnList(
            col_names = ['obama', '2008'],
            col_types = ['text', 'boolean'])
        
        self.assertEqual((columns1 + columns2).col_names,
            ['thanks', 'obama', '2008'])
        
    def test_sub(self):
        # ['capital', 'country', 'currency', 'demonym', 'population']
        columns1 = ColumnList(
            col_names = world_countries_cols(),
            col_types = world_countries_types())
            
        columns2 = ColumnList(
            col_names = ['Capital', 'Country'],
            col_types = ['text', 'text']
        )
        
        self.assertEqual(
            (columns1 - columns2).col_names,
            ['currency', 'demonym', 'population'])
            
    def test_eq_1(self):
        '''
        Test that column lists that have the same names in different 
        order evaluate to 1 under the equality operator
        '''
        
        columns1 = ColumnList(
            col_names = ['thanks', 'obama'],
            col_types = ['text', 'text'])
        columns2 = ColumnList(
            col_names = ['obama', 'thanks'],
            col_types = ['text', 'text'])
        
        self.assertEqual((columns1 == columns2), 1)
    
if __name__ == '__main__':
    unittest.main()