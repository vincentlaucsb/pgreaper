''' Tests for HTML Output '''

import pgreaper

import os
import unittest

class HTMLOutputTest(unittest.TestCase):
    ''' Regression tests for HTML output '''
    
    @staticmethod
    def html_load(html_file):
        ''' Simple helper method which loads HTML files into a Python string '''
        with open(html_file, 'r') as test_file:
            return ''.join(test_file.readlines()).replace('\n', '')
    
    def test_csv_in(self):
        pgreaper.csv_to_html(
            os.path.join('data', 'us_states.csv'),
            'us_states.html')
            
        self.assertEqual(
            self.html_load('us_states.html'),
            self.html_load(os.path.join('data', 'us_states.html'))
        )
        
    def test_text_in(self):
        pgreaper.text_to_html(
            os.path.join('data', 'tab_delim.txt'),
            'tab_delim.html')
            
        self.assertEqual(
            self.html_load('tab_delim.html'),
            self.html_load(os.path.join('data', 'tab_delim.html'))
        )
        
    @classmethod
    def tearDownClass(cls):
        os.remove('us_states.html')
        os.remove('tab_delim.html')
        
if __name__ == '__main__':
    unittest.main()