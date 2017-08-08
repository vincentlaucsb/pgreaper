import sqlify
from sqlify import Table
from sqlify.testing import *
from sqlify.core import _core
from sqlify.core.schema import DialectSQLite

# from _shared import *

def world_countries_cols():
    return ['Capital', 'Country', 'Currency', 'Demonym', 'Population']
                   
def world_countries():
    return [["Washington", "USA", "USD", 'American', "324774000"],
            ["Moscow", "Russia", "RUB", 'Russian', "144554993"],
            ["Ottawa", "Canada", "CAD", 'Canadian', "35151728"]]
            
def world_countries_table():
    return Table('Countries',
        col_names = world_countries_cols(),
        row_values = world_countries()
    )
    
from collections import OrderedDict
import copy
import unittest
import os

class TableTest(unittest.TestCase):
    ''' Test if the Table class is working correctly '''
    
    # Test if columns get converted to rows successfully
    def test_row_values(self):
        col_names = ["Capital", "Country"]
        col_values = [["Washington", "Moscow", "Ottawa"],
                      ["USA", "Russia", "Canada"]]
    
        output = Table('Capitals', col_names=col_names, col_values=col_values)
        expected_output = [["Washington", "USA"],
                           ["Moscow", "Russia"],
                           ["Ottawa", "Canada"]]
                                       
        self.assertEqual(output, expected_output)
        
    def test_pkey_swap(self):
        ''' Test if changing the primary key also changes col_types '''
        col_names = ["Capital", "Country"]
        col_values = [["Washington", "Moscow", "Ottawa"],
                      ["USA", "Russia", "Canada"]]
    
        output = Table('Capitals', col_names=col_names,
            col_values=col_values, p_key=0)
        
        # Change primary key
        output.p_key = 1
                                       
        self.assertNotIn('PRIMARY KEY', output.col_types[0])
        self.assertIn('PRIMARY KEY', output.col_types[1])
        
    def test_pkey_swap2(self):
        ''' Test that setting primary key multiple times doesn't cause errors '''
        col_names = ["Capital", "Country"]
        col_values = [["Washington", "Moscow", "Ottawa"],
                      ["USA", "Russia", "Canada"]]
    
        output = Table('Capitals', col_names=col_names,
            col_values=col_values, p_key=0)
            
        output.p_key = 1
        output.p_key = 1
        output.p_key = 1
        
        self.assertEqual(output.col_types[1], 'TEXT PRIMARY KEY')

    @unittest.skip("Need to revise this test")
    # Test if na_value removal works
    def test_na_rm(self):
        output = sqlify.csv_to_table(
            os.path.join('data', 'SP500.csv'), header=0, na_values='.')
        
        # Value corresponding to '4/6/2007'
        self.assertEqual(output[19][1], None)
        
class TransformTest(unittest.TestCase):
    ''' Test if functions for transforming tables work properly '''
    
    # Just the population column
    population = [["324774000"], ["144554993"], ["35151728"]]
    
    def fun_func(entry):
        ''' Replaces all occurrences of "Canada" with "Canuckistan" '''
        
        if entry == 'Canada':
            return 'Canuckistan'
        
        return entry
    
    def setUp(self):
        self.tbl = Table('Countries',
            col_names=world_countries_cols(),
            row_values=world_countries())
            
        self.tbl.col_types = self.tbl.guess_type()
        
    def test_apply1(self):
        # Test that apply function with string col argument works
        self.tbl.apply('Country', func=TransformTest.fun_func)
        
        correct = [["Washington", "USA", "USD", 'American', "324774000"],
                   ["Moscow", "Russia", "RUB", 'Russian', "144554993"],
                   ["Ottawa", "Canuckistan", "CAD", 'Canadian', "35151728"]]
        
        self.assertEqual(self.tbl, correct)
        
    def test_apply2(self):
        # Test that apply function with index col argument works
        self.tbl.apply(1, func=TransformTest.fun_func)
        
        correct = [["Washington", "USA", "USD", 'American', "324774000"],
                   ["Moscow", "Russia", "RUB", 'Russian', "144554993"],
                   ["Ottawa", "Canuckistan", "CAD", 'Canadian', "35151728"]]
        
        self.assertEqual(self.tbl, correct)
        
    def test_mutate(self):
        # Test that mutate function works
        self.tbl.mutate('ActualCountry', TransformTest.fun_func, 'Country')
        
        correct = [["Washington", "USA", "USD", 'American', "324774000", "USA"],
                   ["Moscow", "Russia", "RUB", 'Russian', "144554993", "Russia"],
                   ["Ottawa", "Canada", "CAD", 'Canadian', "35151728", "Canuckistan"]]
                   
        self.assertEqual(self.tbl, correct)
        
    def test_reorder_shrink(self):
        # Test that reorder with an intended smaller output table works
        new_tbl = self.tbl.reorder('Country', 'Population')
        
        correct = [["USA", "324774000"],
                   ["Russia", "144554993"],
                   ["Canada", "35151728"]]

        self.assertEqual(new_tbl, correct)
        
    def test_reorder_mixed_args(self):
        # Test reorder with a mixture of indices and column name arguments
        new_tbl = self.tbl.reorder(3, 'Currency', 0)
        
        correct = [['American', "USD", "Washington"],
                   ['Russian', "RUB", "Moscow"],
                   ["Canadian", "CAD", "Ottawa"]]
        
        self.assertEqual(new_tbl, correct)
        
    def test_label(self):
        ''' Test that adding a label works '''
        self.tbl.label(col="dataset", label="dataset-1")
        
        correct = world_countries()
        
        for row in correct:
            row.append("dataset-1")
            
        self.assertEqual(self.tbl.col_names[-1], 'dataset')
        self.assertEqual(self.tbl, correct)
    
    def test_get_col(self):
        ''' Test that getting columns by key works'''
        col = self.tbl['Population']
        self.assertEqual(col, ["324774000", "144554993", "35151728"])
    
    def test_subset(self):
        ''' Test that Table subsetting works '''
        new_tbl = self.tbl.subset('Population')
        self.assertEqual(new_tbl, TransformTest.population)        
        
    def test_delete(self):
        ''' Test that deleting a column works '''
        # Valid comparison as long as test_subset() passes
        compare_tbl = copy.deepcopy(self.tbl).subset('Country')
        
        self.tbl.delete('Capital')
        self.tbl.delete('Population')
        self.tbl.delete('Currency')
        self.tbl.delete('Demonym')
        
        self.assertEqual(self.tbl, compare_tbl)
        
class TextToTable(unittest.TestCase):
    ''' Test if text files are being converted to tables properly '''
    
    # Test if tab-delimited files are being converted succesfully
    def test_tab(self):
        output = sqlify.text_to_table(
            file=os.path.join('data', 'tab_delim.txt'), delimiter='\t')
        expected_output = [['Washington', 'USA'],
                           ['Moscow', 'Russia'],
                           ['Ottawa', 'Canada']]

        self.assertEqual(output, expected_output)

class TableReprTest(unittest.TestCase):
    ''' Spot tests to see if Table string representation works '''
    
    data = world_countries_table()
    
    def test_repr_nxm(self):
        ''' Test that row and column count is included '''
        self.assertIn('3 rows x 5 columns',
            TableReprTest.data.__repr__())
    
    def test_html(self):
        self.assertIn('<h2>Countries</h2>',
            TableReprTest.data._repr_html_())
        
class HelpersTest(unittest.TestCase):
    ''' Test if helper functions work correctly '''
    
    def test_strip_badchar(self):
        # Test if _strip function fixes names containing bad characters
        input = 'asdf;bobby_tables'
        expected_output = 'asdf_bobby_tables'
        
        self.assertEqual(_core.strip(input), expected_output)
    
    def test_strip_numeric(self):
        # Test if _strip function fixes names that begin with numbers
        input = '123_bad_name'
        expected_output = '_123_bad_name'
        
        self.assertEqual(_core.strip(input), expected_output)

class GuessTableTest(unittest.TestCase):
    ''' Test if data type guesser is reasonably accurate for Tables '''
    
    def test_simple_case(self):
        tbl = world_countries_table()
        tbl.guess_type()
        
        self.assertEqual(
            tbl.col_types,
            ['TEXT', 'TEXT', 'TEXT', 'TEXT', 'INTEGER'])
                        
    def test_mixed_case(self):
        col_names = ['mixed_data', 'mixed_numbers', 'just_int']
        row_values = [['abcd', '123', '123'],
                      ['bcda', '1.23', '222'],
                      ['1111', '3.14', '222']]
        
        tbl = Table('Some Data', col_names=col_names, row_values=row_values)
        tbl.guess_type()
        
        expected_col_types = ['TEXT', 'REAL', 'INTEGER']
        self.assertEqual(tbl.col_types, expected_col_types)        

if __name__ == '__main__':
    unittest.main()