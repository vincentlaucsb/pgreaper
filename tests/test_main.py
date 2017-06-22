import sqlify
from sqlify import table
from sqlify.table import Table
from sqlify import helpers

from collections import OrderedDict
import unittest
import os

class TableTest(unittest.TestCase):
    '''
    Test if the Table class is working correctly
    '''
    
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
        
    # Test if changing the primary key also changes col_types
    def test_pkey_swap(self):
        col_names = ["Capital", "Country"]
        col_values = [["Washington", "Moscow", "Ottawa"],
                      ["USA", "Russia", "Canada"]]
    
        output = Table('Capitals', col_names=col_names,
            col_values=col_values, p_key=0)
        
        # Change primary key
        output.p_key = 1
                                       
        self.assertNotIn('PRIMARY KEY', output.col_types[0])
        self.assertIn('PRIMARY KEY', output.col_types[1])

    # Test if na_value removal works
    def test_na_rm(self):
        output = sqlify.csv_to_table(
            os.path.join('data', 'SP500.csv'), header=0, na_values='.')
        
        # Value corresponding to '4/6/2007'
        self.assertEqual(output[19][1], None)
        
class TextToTable(unittest.TestCase):
    ''' Test if text files are being converted to tables properly '''
    
    # Test if tab-delimited files are being converted succesfully
    def test_tab(self):
        output = sqlify.text_to_table(file='tab_delim.txt', delimiter='\t')
        expected_output = [['Washington', 'USA'],
                           ['Moscow', 'Russia'],
                           ['Ottawa', 'Canada']]

        self.assertEqual(output, expected_output)

class HelpersTest(unittest.TestCase):
    ''' Test if helper functions work correctly '''
    
    def test_strip_badchar(self):
        # Test if _strip function fixes names containing bad characters
        input = 'asdf;bobby_tables'
        expected_output = 'asdf_bobby_tables'
        
        self.assertEqual(helpers._strip(input), expected_output)
    
    def test_strip_numeric(self):
        # Test if _strip function fixes names that begin with numbers
        input = '123_bad_name'
        expected_output = '_123_bad_name'
        
        self.assertEqual(helpers._strip(input), expected_output)
        
class GuessTest(unittest.TestCase):
    ''' Test if data type guesser is reasonably accurate '''
    
    def test_obvious_case1(self):
        input = '3.14'
        output = table._guess_data_type(input)
        expected_output = 'REAL'
        
        self.assertEqual(output, expected_output)
        
    def test_obvious_case2(self):
        input = 'Tom Brady'
        output = table._guess_data_type(input)
        expected_output = 'TEXT'
        
        self.assertEqual(output, expected_output)
        
    def test_obvious_case3(self):
        input = '93117'
        output = table._guess_data_type(input)
        expected_output = 'INTEGER'
        
        self.assertEqual(output, expected_output)

class GuessTableTest(unittest.TestCase):
    ''' Test if data type guesser is reasonably accurate for Tables '''
    
    def test_simple_case(self):
        col_names = ['Capital', 'Country', 'Currency', 'Demonym', 'Population']
        row_values = [["Washington", "USA", "USD", 'American', "324774000"],
                      ["Moscow", "Russia", "RUB", 'Russian', "144554993"],
                      ["Ottawa", "Canada", "CAD", 'Canadian', "35151728"]]
        
        tbl = Table('Countries', col_names=col_names, row_values=row_values)
        
        expected_col_types = ['TEXT', 'TEXT', 'TEXT', 'TEXT', 'INTEGER']
        
        self.assertEqual(tbl.guess_type(), expected_col_types)
                        
    def test_mixed_case(self):
        col_names = ['mixed_data', 'mixed_numbers', 'just_int']
        row_values = [['abcd', '123', '123'],
                      ['bcda', '1.23', '222'],
                      ['1111', '3.14', '222']]
        
        tbl = Table('Some Data', col_names=col_names, row_values=row_values)
        
        expected_col_types = ['TEXT', 'REAL', 'INTEGER']
        
        self.assertEqual(tbl.guess_type(), expected_col_types)        
                        
if __name__ == '__main__':
    unittest.main()