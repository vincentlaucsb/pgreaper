''' Tests of the core Table data structure '''

from pgreaper import Table
from pgreaper.testing import *
import pgreaper

from collections import OrderedDict

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
                                       
        self.assertNotIn('primary key', output.col_types[0])
        self.assertIn('primary key', output.col_types[1])
        
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
        
        self.assertEqual(output.col_types[1], 'text primary key')
        
    def test_slice(self):
        ''' Test that slicing works '''
        table = world_countries_table()
        table[1:3]
        
class ColumnIndexTest(unittest.TestCase):
    ''' Test that column name error handling works '''
    
    def setUp(self):
        self.data = world_countries_table()
    
    def test_invalid_colname(self):
        with self.assertRaises(ValueError):
            self.data._parse_col(None)
            
    def test_not_found(self):
        with self.assertRaises(KeyError):
            self.data._parse_col('sasquatch')

class AppendTest(unittest.TestCase):
    ''' Test if functions for adding to a Table work '''
    
    def test_from_nothing(self):
        ''' Test that adding to a Table with no columns or rows works '''
        table = pgreaper.Table(name=None)
        table.add_dicts(
            [{
                "Capital": "Beijing",
                "Country": "China",
                "Demonym": 'Chinese',
                "Population": 1373541278,
             }])

        self.assertEqual(table['Capital'], ['Beijing'])
    
    def test_add_smaller_dict(self):
        table = world_countries_table()
        table.add_dicts(
            [{
                "Capital": "Beijing",
                "Country": "China",
                "Demonym": 'Chinese',
                "Population": 1373541278,
             }])
             
        # Test that currency column was filled with None
        self.assertEqual(table['Currency'], ['USD', 'RUB', 'CAD', None])
    
    def test_add_bigger_dict(self):
        table = world_countries_table()
        table.add_dicts(
            [{
                "Capital": "Beijing",
                "Country": "China",
                "Currency": "CNY",
                "Demonym": 'Chinese',
                "Population": 1373541278,
                "GDP": "$23.2 trillion"
             }])
             
        # Test that extra "GDP" column was added
        self.assertEqual(table['GDP'], [None, None, None, "$23.2 trillion"])
        
    def test_extract(self):
        table = world_countries_table()
        table.add_dicts(
            [{
                "Capital": "Beijing",
                "Country": "China",
                "Currency": "CNY",
                "Demonym": 'Chinese',
                "Population": 1373541278,
                "GDP": "$23.2 trillion"
             }])
             
        # Test that extra "GDP" column was added
        self.assertEqual(table['GDP'], [None, None, None, "$23.2 trillion"])
        
class TransformTest(unittest.TestCase):
    ''' Test if functions for transforming tables work properly '''
    
    # Just the population column
    population = [[324774000], [144554993], [35151728]]
    
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
        
        correct = [["Washington", "USA", "USD", 'American', 324774000],
                   ["Moscow", "Russia", "RUB", 'Russian', 144554993],
                   ["Ottawa", "Canuckistan", "CAD", 'Canadian', 35151728]]
        
        self.assertEqual(self.tbl, correct)
        
    def test_apply2(self):
        # Test that apply function with index col argument works
        self.tbl.apply(1, func=TransformTest.fun_func)
        
        correct = [["Washington", "USA", "USD", 'American', 324774000],
                   ["Moscow", "Russia", "RUB", 'Russian', 144554993],
                   ["Ottawa", "Canuckistan", "CAD", 'Canadian', 35151728]]
        
        self.assertEqual(self.tbl, correct)
        
    def test_mutate(self):
        # Test that mutate function works
        self.tbl.mutate('ActualCountry', TransformTest.fun_func, 'Country')
        
        correct = [["Washington", "USA", "USD", 'American', 324774000, "USA"],
                   ["Moscow", "Russia", "RUB", 'Russian', 144554993, "Russia"],
                   ["Ottawa", "Canada", "CAD", 'Canadian', 35151728, "Canuckistan"]]
                   
        self.assertEqual(self.tbl, correct)
        
    def test_reorder_shrink(self):
        # Test that reorder with an intended smaller output table works
        new_tbl = self.tbl.reorder('Country', 'Population')
        
        correct = [["USA", 324774000],
                   ["Russia", 144554993],
                   ["Canada", 35151728]]

        self.assertEqual(new_tbl, correct)
        
    def test_reorder_mixed_args(self):
        # Test reorder with a mixture of indices and column name arguments
        new_tbl = self.tbl.reorder(3, 'Currency', 0)
        
        correct = [['American', "USD", "Washington"],
                   ['Russian', "RUB", "Moscow"],
                   ["Canadian", "CAD", "Ottawa"]]
        
        self.assertEqual(new_tbl, correct)
        
    def test_add_col(self):
        ''' Test that adding a label works '''
        self.tbl.add_col(col="dataset", fill="dataset-1")
        
        correct = world_countries()
        
        for row in correct:
            row.append("dataset-1")
            
        self.assertEqual(self.tbl.col_names[-1], 'dataset')
        self.assertEqual(self.tbl, correct)
    
    def test_get_col(self):
        ''' Test that getting columns by key works'''
        col = self.tbl['Population']
        self.assertEqual(col, [324774000, 144554993, 35151728])
    
    def test_subset(self):
        ''' Test that Table subsetting works '''
        new_tbl = self.tbl.subset('Population')
        self.assertEqual(new_tbl, TransformTest.population)        
        
    def test_transpose(self):
        new_tbl = self.tbl.transpose(include_header=False)     
        self.assertEqual(new_tbl, pgreaper.Table(
            name = 'Countries',
            row_values = [['Washington', 'Moscow', 'Ottawa'],
                         ['USA', 'Russia', 'Canada'],
                         ['USD', 'RUB', 'CAD'],
                         ['American', 'Russian', 'Canadian'],
                         [324774000, 144554993, 35151728]]))
        
    def test_drop_empty(self):
        # Add 5 empty rows
        new_tbl = copy.deepcopy(self.tbl)
        
        for i in range(0, 5):
            new_tbl.append([] * new_tbl.n_cols)
            
        new_tbl.drop_empty()
        self.assertEqual(new_tbl, self.tbl)
        
    def test_delete(self):
        ''' Test that deleting a column works '''
        # Valid comparison as long as test_subset() passes
        compare_tbl = copy.deepcopy(self.tbl).subset('Country')
        
        self.tbl.delete('Capital')
        self.tbl.delete('Population')
        self.tbl.delete('Currency')
        self.tbl.delete('Demonym')
        
        self.assertEqual(self.tbl, compare_tbl)
        
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
        
        self.assertEqual(pgreaper._globals.strip(input), expected_output)
    
    def test_strip_numeric(self):
        # Test if _strip function fixes names that begin with numbers
        input = '123_bad_name'
        expected_output = '_123_bad_name'
        
        self.assertEqual(pgreaper._globals.strip(input), expected_output)
        
class PrimaryKeyTest(unittest.TestCase):
    '''
     - Test that composite primary keys are handled correctly
     - Test that ColumnList responds to nonsense with an appropriate
       error message
    '''
    
    def setUp(self):
        self.data = world_countries_table()
        self.data.add_col('Year', 2017)
        self.data.p_key = ('Country', 'Year')
        
    def test_tuple_str(self):
        ''' Make sure tuple[str] are converted correctly '''
        self.assertEqual(self.data.p_key, (1, 5))
        
    def test_repr(self):
        self.assertIn('Composite Primary Key: Country, Year',
            self.data.__repr__())
            
    def test_repr_html(self):
        self.assertIn('Composite Primary Key: Country, Year',
            self.data._repr_html_())
        
    def test_index_error1(self):
        with self.assertRaises(IndexError):
            self.data.p_key = 420
            
    def test_index_error2(self):
        with self.assertRaises(IndexError):
            self.data.p_key = (0, 10)
            
    def test_dne(self):
        with self.assertRaises(KeyError):
            self.data.p_key = ('Country', 'Harambe')
        
if __name__ == '__main__':
    unittest.main()