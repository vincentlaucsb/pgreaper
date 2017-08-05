''' Tests for converting files to CSV '''

import sqlify

import unittest
import json
import os

class CSVOutputTest(unittest.TestCase):
    ''' Test that CSV files are being outputted correctly '''
        
    @staticmethod
    def csv_load(csv_file):
        ''' Simple helper method which loads CSV files into a Python string '''
        with open(csv_file, 'r') as test_file:
            return ''.join(test_file.readlines()).replace('\n', '')
        
    def test_from_txt(self):
        '''
        Test if a simple TXT file is converted correctly by
        comparing it to a previous output which has been manually
        inspected
        '''
        
        sqlify.text_to_csv('data/tab_delim.txt', 'tab_delim_test.csv')        
        self.assertEqual(self.csv_load('tab_delim_test.csv'),
            self.csv_load('data/tab_delim.csv'))
            
    @classmethod
    def tearDownClass(cls):
        os.remove('tab_delim_test.csv')
        
if __name__ == '__main__':
    unittest.main()