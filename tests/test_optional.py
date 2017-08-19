''' Test that optional dependencies are optional '''

import unittest
import os
import configparser
import sqlify

class ErrorTest(unittest.TestCase):
    ''' Test that the correct error messages for missing packages are displayed '''
    
    def test_html(self):
        with self.assertRaises(ImportError):
            sqlify.html.from_url('https://stackoverflow.com')        
        
    def test_pandas(self):
        with self.assertRaises(ImportError):
            sqlify.pandas_to_table([])
            
    def test_alchemy(self):
        with self.assertRaises(ImportError):
            x = sqlify.SQLTable()

if __name__ == '__main__':
    if not(sqlify._globals.import_package('pandas')):
        unittest.main()