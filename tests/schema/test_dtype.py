''' Test if ColumnList works '''

from pgreaper._globals import PG_KEYWORDS
from pgreaper.testing import *
from pgreaper.core import ColumnList
from pgreaper.core.schema import SQLType
import pgreaper

from functools import partial
import unittest

####################
# ColumnList Tests #
####################

class TestColumns(unittest.TestCase):
    ''' Test that this helper data structure works '''
    
    def setUp(self):
        self.columns1 = ColumnList(col_names = ['thanks', 'OBaMa'])

    #################
    # Helper Checks #
    #################
    
    def test_index(self):
        self.assertEqual(self.columns1.index('thanks'), 0)
    
    def test_not_in_list(self):
        with self.assertRaises(KeyError):
            self.columns1.index('georgebush') 
            
    #########################
    # Input Checks          #
    #########################
        
    def test_invalid_pkey(self):
        with self.assertRaises(TypeError):
            self.columns1.p_key = set()       
            
    #############################
    # "Mathematical Operations" #
    #############################
    
    def test_smaller_input(self):
        # Test that map() function works correctly
        self.assertEqual(self.columns1.map('thanks'),
            {0: 'thanks'})
            
    def test_bigger_input(self):
        # Test that map() function doesn't try to map non-existent keys
        self.assertEqual(self.columns1.map('thanks', 'alex', 'jones'),
            {0: 'thanks'})
    
    def test_lower(self):
        # Assert that comparison is case insensitive
        columns1 = ColumnList(col_names = ['thanks', 'OBaMa'])
        columns2 = ColumnList(col_names = ['thanks', 'obama'])
        
        self.assertEqual((columns1 == columns2), 2)
        
    def test_simple_add(self):
        ''' Test that + preserves order of first summand '''
        
        columns1 = ColumnList(col_names = ['thanks', 'obama'])
        columns2 = ColumnList(col_names = ['obama', '2008'])
        
        self.assertEqual((columns1 + columns2).col_names,
            ['thanks', 'obama', '2008'])
        
    def test_sub(self):
        # ['capital', 'country', 'currency', 'demonym', 'population']
        columns1 = ColumnList(col_names = world_countries_cols())
        columns2 = ColumnList(col_names = ['Capital', 'Country'])
        
        expected_result = ColumnList(col_names=['Currency', 'Demonym', 'Population'])
        
        self.assertEqual(columns1 - columns2, expected_result)
        
    def test_div(self):
        # Test that "division" works        
        columns1 = ColumnList(
            col_names = world_countries_cols(),
            col_types = ['text', 'text', 'text', 'bigint', 'boolean'])
        columns2 = ColumnList(
            col_names = world_countries_cols(),
            col_types = ['text', 'bigint', 'text', 'double precision', 'boolean'])
            
        expected_result = ColumnList(
            col_names = ['Country', 'Demonym'],
            col_types = ['text', 'bigint'])
        self.assertEqual(columns1/columns2, expected_result)
            
    def test_eq_1(self):
        '''
        Test that column lists that have the same names in different 
        order evaluate to 1 under the equality operator
        '''
        
        columns1 = ColumnList(col_names = ['thanks', 'obama'])
        columns2 = ColumnList(col_names = ['obama', 'thanks'])
        self.assertEqual((columns1 == columns2), 1)
        
    def test_less_than(self):
        ''' Test that column lists are case insensitive w.r.t. < '''
        
        columns1 = ColumnList(col_names = ['thaNks', 'oBaMA'])
        columns2 = ColumnList(col_names = ['thanks', 'alot', 'obama'])
        self.assertTrue(columns1 < columns2)
    
    def test_greater_than(self):
        ''' Test that column lists are case insensitive w.r.t. > '''
        
        columns1 = ColumnList(col_names = ['thanks', 'alot', 'obama'])
        columns2 = ColumnList(col_names = ['thaNks', 'oBaMA'])
        self.assertTrue(columns1 > columns2)
        
class TestSanitize(unittest.TestCase):
    ''' Test that the sanitize() method works '''
    
    def setUp(self):
        self.data = world_countries_table()
        self.data.dialect = 'postgres'
        
        # Mangle column names
        self.data.col_names = ['User', 'Table', 'Column', 'Check', 'Analyze']
        
    def test_reserved(self):
        ''' Test that trailing underscores were appended '''       
        self.assertEqual(self.data.columns.sanitize(PG_KEYWORDS),
            ['user_', 'table_', 'column_', 'check_', 'analyze_'])
            
    def test_reserved2(self):
        ''' Test that trailing underscores were appended '''       
        self.assertEqual(self.data.col_names_sanitized,
            ['user_', 'table_', 'column_', 'check_', 'analyze_'])

#####################
# Integration Tests #
#####################

class IntegrityTest(unittest.TestCase): 
    '''
    Make sure that methods like reorder() and apply() don't mangle
    type inference
    '''
        
    def setUp(self):
        self.table = world_countries_table()
        self.table.dialect = 'sqlite'

    def test_reorder(self):
        ''' Test that reordering doesn't mess up column types '''
        new_table = self.table.reorder('Currency', 'Population')
        self.assertEqual(new_table.col_types,
            ['text', 'integer'])
        
    def test_add_col(self):
        ''' Test that adding a column doesn't mess up column types '''
        self.table.add_col('Dataset', fill='World Countries')
        self.table.guess_type()
        self.assertEqual(self.table.col_types,
            ['text', 'text', 'text', 'text', 'integer', 'text'])
            
    def test_add_col_reorder(self):
        '''
        Test that adding a column and reordering doesn't mess up
        column types
        '''
        self.table.add_col('Year', fill=2017)
        new_table = self.table.reorder('Population', 'Year')
        self.assertEqual(new_table.col_types, ['integer', 'integer'])
        
class RenameTest(unittest.TestCase):
    ''' Test that column renaming doesn't affect type counter '''
    
    pass
        
if __name__ == '__main__':
    unittest.main()