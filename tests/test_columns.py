''' Test if ColumnList helper class works '''

from sqlify.testing import *
from sqlify.core import ColumnList

from functools import partial
import unittest

class TestColumns(unittest.TestCase):
    ''' Test that this helper data structure works '''
    
    def setUp(self):
        self.columns1 = ColumnList(
            col_names = ['thanks', 'OBaMa'],
            col_types = ['text', 'text'])

    #################
    # Helper Checks #
    #################
    
    def test_index(self):
        self.assertEqual(self.columns1.index('thanks'), 0)
    
    def test_not_in_list(self):
        with self.assertRaises(KeyError):
            self.columns1.index('georgebush') 
            
    def test_col_types(self):
        self.assertEqual(self.columns1.col_types,
            ['text', 'text'])
            
    #########################
    # Input Checks          #
    #########################
    
    def test_same_type(self):
        columns = ColumnList(
            col_names = ['alex', 'jones', 'shut', 'down', 'govt'],
            col_types = 'text')
        self.assertEqual(columns.col_types, ['text'] * 5)
        
    def test_invalid_pkey(self):
        with self.assertRaises(TypeError):
            self.columns1.p_key = set()       
            
    #############################
    # "Mathematical Operations" #
    #############################
    
    def test_lower(self):
        # Assert that comparison is case insensitive
        columns1 = ColumnList(
            col_names = ['thanks', 'OBaMa'],
            col_types = ['text', 'text'])
        columns2 = ColumnList(
            col_names = ['thanks', 'obama'],
            col_types = ['text', 'text'])
        
        self.assertEqual((columns1 == columns2), 2)
        
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
            ['Currency', 'Demonym', 'Population'])
            
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