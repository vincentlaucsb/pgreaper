import sqlify

import unittest
import json
import os

class JSONOutputTest(unittest.TestCase):
    ''' Test that JSONs are being outputted correctly '''
    
    def test_output(self):
        '''
        Compare output vs a previously ouputted JSON which has been
        manually inspected for correctness
        '''
        
        sqlify.csv_to_json('data/us_states.csv', 'us_states_test.json')
        
        with open('us_states_test.json', 'r') as test_file:
            test_json = ''.join(test_file.readlines()).replace('\n', '')
        
        test_json = json.loads(test_json)
        
        with open('data/us_states.json', 'r') as compare_file:
            compare_json = ''.join(compare_file.readlines()).replace('\n', '')
            
        compare_json = json.loads(compare_json)
        
        self.assertEqual(test_json, compare_json)
        
    @classmethod
    def tearDown(cls):
        os.remove('us_states_test.json')

if __name__ == '__main__':
    unittest.main()