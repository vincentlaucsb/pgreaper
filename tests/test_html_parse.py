''' Tests for HTML Parsing '''

import pgreaper

import os
import unittest

class HTMLTableTest(unittest.TestCase):
    ''' Test HTML parsing of <table>s '''
    
    correct_col_names = ['Capital', 'Country', 'Population']
    correct_first_row = ['Washington', 'USA', '324774000']
    
    @classmethod
    def setUpClass(cls):
        # Load tables        
        cls.tables = pgreaper.html.from_file(
            os.path.join('data', 'table_test.html'))
    
    def test_no_header(self):
        '''
        Header not correctly specified
        
         - Make sure placeholder names are used
         
        '''
        
        table = HTMLTableTest.tables[0]
        
        self.assertEqual(table.name, "FDR")
        self.assertEqual(table.col_names,
            ['col0', 'col1', 'col2'])
        self.assertEqual(table[1],
            HTMLTableTest.correct_first_row)
            
    def test_th(self):
        ''' No tbody or thead but a row of th is present '''
        
        table = HTMLTableTest.tables[1]
        
        self.assertEqual(table.name, "Winston Churchill")
        self.assertEqual(table.col_names,
            HTMLTableTest.correct_col_names)
        self.assertEqual(table[0],
            HTMLTableTest.correct_first_row)
            
    def test_thead_tbody(self):
        ''' Contains thead and tbody '''
        
        table = HTMLTableTest.tables[2]
        
        self.assertEqual(table.name, "Joseph Stalin")
        self.assertEqual(table.col_names,
            HTMLTableTest.correct_col_names)
        self.assertEqual(table[0],
            HTMLTableTest.correct_first_row)
            
    def test_caption1(self):
        ''' Contains caption, thead and tbody '''
        
        table = HTMLTableTest.tables[3]

        self.assertEqual(table.name, "Charles de Gaulle")
    
    def test_caption2(self):
        table = HTMLTableTest.tables[3]
    
        self.assertEqual(table.col_names,
            HTMLTableTest.correct_col_names)
        self.assertEqual(table[0],
            HTMLTableTest.correct_first_row)
            
    def test_tbody(self):
        ''' Contains tbody but no thead '''
        
        table = HTMLTableTest.tables[4]
        
        self.assertEqual(table.name, "Chiang Kai-shek")
        self.assertEqual(table.col_names,
            HTMLTableTest.correct_col_names)
        self.assertEqual(table[0],
            HTMLTableTest.correct_first_row)
            
    @unittest.skip("Not implemented")
    def test_direct_input(self):
        ''' Test that converting HTML from direct input works '''
        
        html_code = '''<h2>Country Info</h2>
            <table>
                <tbody>
                    <tr>
                        <th>Capital</th><th>Country</th><th>Population</th>
                    </tr><tr>
                        <td>Washington</td><td>USA</td><td>324774000</td>
                    </tr><tr>
                        <td>Moscow</td><td>Russia</td><td>144554993</td>
                    </tr><tr>
                        <td>Ottawa</td><td>Canada</td><td>35151728</td>
                    </tr>
                </tbody>
            </table>
        '''
        
        tables = pgreaper.get_tables(html_code)
        # import pdb; pdb.set_trace()
        table = tables[0]
        
        self.assertEqual(table.name, "Country Info")
        self.assertEqual(table.col_names,
            HTMLTableTest.correct_col_names)
        self.assertEqual(table[0],
            HTMLTableTest.correct_first_row)

class HTMLComplexTableTest(unittest.TestCase):
    def test_complex_table(self):
        ''' Test a table whose cells have multiple colspan and rowspan arguments '''
        
        table = pgreaper.html.from_file(
            os.path.join('data', 'complex_table_test.html'))[0]
            
        correct = [['Normal', 'Wide', 'Wide', 'Normal'],
                   ['Normal', 'Long', 'Fat', 'Fat'],
                   ['Normal', 'Long', 'Fat', 'Fat'],
                   ['Normal', 'Long', 'Fat', 'Fat'],
                   ['Normal', 'Normal', 'Fat', 'Fat'],
                   ['Normal', 'Normal', 'Normal', 'Normal']]
            
        self.assertEqual(table.name, "Your Parser Sucks")
        self.assertEqual(table.col_names, ['Apple', 'Banana', 'Guava', 'Orange'])
        self.assertEqual(table, correct)
            
if __name__ == '__main__':
    unittest.main()