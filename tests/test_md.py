''' Tests for converting files to Markdown '''

import sqlify

import unittest
import os

class MDOutputTest(unittest.TestCase):
    ''' Test that Markdown files are being outputted correctly '''
        
    @staticmethod
    def md_load(csv_file):
        ''' Simple helper method which loads MD files into a Python string '''
        with open(csv_file, 'r') as test_file:
            return ''.join(test_file.readlines()).replace('\n', '')
        
    def test_from_csv(self):
        '''
        Test if a simple CSV file is converted correctly by comparing 
        it to one that has been manually inspected for correctness
        '''
        
        sqlify.csv_to_md('data/us_states.csv', 'us_states_csv_test.md')
        self.maxDiff = None
        self.assertEqual(self.md_load('us_states_csv_test.md'),
            self.md_load('data/us_states.md'))
            
    @classmethod
    def tearDownClass(cls):
        os.remove('us_states_csv_test.md')
        # os.remove('us_states_txt_test.md')
        
if __name__ == '__main__':
    unittest.main()