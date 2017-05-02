import sqlify
from sqlify import table
from sqlify import helpers

from collections import OrderedDict
import unittest

class TableTest(unittest.TestCase):
    '''
    Test if the Table class is working correctly
    '''
    
    # Test if columns get converted to rows successfully
    def test_row_values(self):
        col_names = ["Capital", "Country"]
        col_values = [["Washington", "Moscow", "Ottawa"],
                      ["USA", "Russia", "Canada"]]
    
        output = sqlify.Table('Capitals', col_names=col_names, col_values=col_values)
        expected_output = [["Washington", "USA"],
                           ["Moscow", "Russia"],
                           ["Ottawa", "Canada"]]
                                       
        self.assertEqual(output, expected_output)
        
    # Test if changing the primary key also changes col_types
    def test_pkey_swap(self):
        col_names = ["Capital", "Country"]
        col_values = [["Washington", "Moscow", "Ottawa"],
                      ["USA", "Russia", "Canada"]]
    
        output = sqlify.Table('Capitals', col_names=col_names,
            col_values=col_values, p_key=0)
        
        # Change primary key
        output.p_key = 1
                                       
        self.assertNotIn('PRIMARY KEY', output.col_types[0])
        self.assertIn('PRIMARY KEY', output.col_types[1])

class ColumnKeyTest(unittest.TestCase):
    ''' Test if using column names as keys are working correctly '''
    
    # Test if columns are being retrieved correctly
    def test_col_values(self):
        col_names = ["Capital", "Country"]
        col_values = [["Washington", "Moscow", "Ottawa"],
                      ["USA", "Russia", "Canada"]]
    
        table = sqlify.Table('Capitals', col_names=col_names, col_values=col_values)
        
        output = table['Capital']
        expected_output = ['Washington', 'Moscow', 'Ottawa']
        
        self.assertEqual(output, expected_output)
    
    # Test if reassignment using column names as keys works
    def test_col_setitem(self):
        col_names = ["Capital", "Country"]
        col_values = [["Washington", "Moscow", "Ottawa"],
                      ["USA", "Russia", "Canada"]]
    
        table = sqlify.Table('Capitals', col_names=col_names, col_values=col_values)
        
        col = table['Country']
        
        # Reassign: Change "Russia" to "China"
        col[1] = 'China'
       
        expected_output = [["Washington", "USA"],
                           ["Moscow", "China"],
                           ["Ottawa", "Canada"]]
        
        self.assertEqual(table, expected_output)
        
class TextToTable(unittest.TestCase):
    ''' Test if text files are being converted to tables properly '''
    
    # Test if tab-delimited files are being converted succesfully
    def test_tab(self):
        output = sqlify.text_to_table(file='tab_delim.txt', delimiter='\t')
        expected_output = [['Washington', 'USA'],
                           ['Moscow', 'Russia'],
                           ['Ottawa', 'Canada']]

        self.assertEqual(output, expected_output)
        
class SubsetTest(unittest.TestCase):
    ''' Test if Table subsetting is working correctly '''
    
    # Test if subsetting by column names works correctly
    def test_col_names(self):
        col_names = ['Capital', 'Country', 'Currency']
        row_values = [["Washington", "USA", "USD"],
                      ["Moscow", "Russia", "RUB"],
                      ["Ottawa", "Canada", "CAD"]]
        
        tbl = sqlify.Table('Countries', col_names=col_names, row_values=row_values)
        
        output = table.subset(tbl, 'Country', 'Currency')
        expected_output = [["USA", "USD"],
                           ["Russia", "RUB"],
                           ["Canada", "CAD"]]
                           
        self.assertEqual(output, expected_output)
        
    # Test if subsetting by column indices (tuple] works correctly
    def test_col_tuple(self):
        col_names = ['Capital', 'Country', 'Currency']
        row_values = [["Washington", "USA", "USD"],
                      ["Moscow", "Russia", "RUB"],
                      ["Ottawa", "Canada", "CAD"]]
        
        tbl = sqlify.Table('Countries', col_names=col_names, row_values=row_values)
        
        output = table.subset(tbl, (1, 2))
        expected_output = [["USA", "USD"],
                           ["Russia", "RUB"],
                           ["Canada", "CAD"]]
                           
        self.assertEqual(output, expected_output)
        
    # Test if subsetting by column indices (list) works correctly
    def test_col_list(self):
        col_names = ['Capital', 'Country', 'Currency', 'Demonym', 'Population']
        row_values = [["Washington", "USA", "USD", 'American', "324774000"],
                      ["Moscow", "Russia", "RUB", 'Russian', "144554993"],
                      ["Ottawa", "Canada", "CAD", 'Canadian', "35151728"]]
        
        tbl = sqlify.Table('Countries', col_names=col_names, row_values=row_values)
        
        output = table.subset(tbl, 0, (2, 3))
        expected_output = [["Washington", "USD", "American"],
                           ["Moscow", "RUB", "Russian"],
                           ["Ottawa", "CAD", "Canadian"]]
                           
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
        
        tbl = sqlify.Table('Countries', col_names=col_names, row_values=row_values)
        
        expected_col_types = ['TEXT', 'TEXT', 'TEXT', 'TEXT', 'INTEGER']
        
        self.assertEqual(tbl.col_types, expected_col_types)
                        
    def test_mixed_case(self):
        col_names = ['mixed_data', 'mixed_numbers', 'just_int']
        row_values = [['abcd', '123', '123'],
                      ['bcda', '1.23', '222'],
                      ['1111', '3.14', '222']]
        
        tbl = sqlify.Table('Some Data', col_names=col_names, row_values=row_values)
        
        expected_col_types = ['TEXT', 'REAL', 'INTEGER']
        
        self.assertEqual(tbl.col_types, expected_col_types)        
                        
if __name__ == '__main__':
    unittest.main()