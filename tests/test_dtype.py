''' Test if ColumnList and SQLType helper classes work '''

from sqlify._globals import PG_KEYWORDS
from sqlify.testing import *
from sqlify.core import ColumnList
from sqlify.core.schema import SQLType
import sqlify

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
    
#################
# SQLType Tests #
#################

class AddTest(unittest.TestCase):
    '''
    Test that "adding" SQLTypes produces the correct type, i.e.
    the type that makes them both compatible in the same column
    '''
    
    def setUp(self):
        self.table = sqlify.Table(name='Test', dialect='postgres')
        
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
        tbl.guess_type()
        
        self.assertEqual(
            tbl.col_types,
            ['text', 'text', 'text', 'text', 'integer'])
                        
    def test_mixed_case(self):
        col_names = ['mixed_data', 'mixed_numbers', 'just_int']
        row_values = [['abcd', 123, 123],
                      ['bcda', 1.23, 222],
                      [1111, 3.14, 222]]
        
        tbl = Table('Some Data', col_names=col_names, row_values=row_values)
        tbl.guess_type()
        
        expected_col_types = ['text', 'real', 'integer']
        self.assertEqual(tbl.col_types, expected_col_types)

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
    
    @unittest.skip('Need to revise this test')
    def test_rename(self):
        self.table = world_countries_table()
        
if __name__ == '__main__':
    unittest.main()