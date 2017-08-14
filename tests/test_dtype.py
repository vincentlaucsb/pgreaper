''' Test if ColumnList and SQLType helper classes work '''

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

if __name__ == '__main__':
    unittest.main()