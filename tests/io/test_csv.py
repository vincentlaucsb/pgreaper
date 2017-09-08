''' Tests for reading and writing to CSV '''

from pgreaper.io.csv_reader import clean_line
import pgreaper

import unittest
import os

class CSVParseTest(unittest.TestCase):
    ''' Test taht CSV files are being parsed correctly '''
    
    def test_clean_line(self):
        ''' Test that unnecessary whitespace added to numbers is removed '''
        table = []
        
        clean_line(['test', 'test', 'test', '    -3.14', '3.14', '3   '],
            table)
        self.assertEqual(table,
            [['test', 'test', 'test', -3.14, 3.14, 3]])

# class CSVOutputTest(unittest.TestCase):
    # ''' Test that CSV files are being outputted correctly '''
        
    # @staticmethod
    # def csv_load(csv_file):
        # ''' Simple helper method which loads CSV files into a Python string '''
        # with open(csv_file, 'r') as test_file:
            # return ''.join(test_file.readlines()).replace('\n', '')
        
    # def test_from_txt(self):
        # '''
        # Test if a simple TXT file is converted correctly by
        # comparing it to a previous output which has been manually
        # inspected
        # '''
        
        # pgreaper.text_to_csv('data/tab_delim.txt', 'tab_delim_test.csv')        
        # self.assertEqual(self.csv_load('tab_delim_test.csv'),
            # self.csv_load('data/tab_delim.csv'))
            
    # @classmethod
    # def tearDownClass(cls):
        # os.remove('tab_delim_test.csv')
        
if __name__ == '__main__':
    unittest.main()