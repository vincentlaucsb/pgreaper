''' Tests for converting files to JSON '''

from pgreaper.testing import *
import pgreaper

from os import path
import unittest
import json
import os
import sys

PYTHON_VERSION = sys.version_info[0] + 0.1 * sys.version_info[1] + \
    sys.version_info[2]
    
class PyJSONTest(unittest.TestCase):
    ''' Test that JSON objects in Python are being read correctly '''
    
    # Load fake persons data
    with open(path.join(MIMESIS_DIR, 'persons.json'), mode='r') as infile:
        person_data = ''.join(infile.readlines())
        
    def test_filter(self):
        ''' Test if filter argument works '''
        
        read_json = pgreaper.read_json(PyJSONTest.person_data,
            filter=['full_name', 'age', 'occupation'])

        self.assertEqual(len(read_json['full_name']), 50000)
        self.assertEqual(len(read_json['age']), 50000)
        self.assertEqual(len(read_json['occupation']), 50000)
        
    def test_extract_nested(self):
        ''' Test that extract argument works for nested dicts '''
        
        json_data = [{
            "State": "Alabama",
            "Abbreviation": "AL",
            "PointlessNesting": {
                "Capital": "Montgomery"
        }}, {
            "State": "Alaska",
            "Abbreviation": "AK",
            "PointlessNesting": {
                "Capital": "Juneau"
        }}, {
            "State": "Arizona", "Abbreviation": "AZ"},
        {"State": "Arkansas", "Abbreviation": "AR"},
        {"State": "California", "Abbreviation": "CA"}]
        
        read_json = pgreaper.read_json(json_data,
            flatten='outer', extract={'Capital': 'PointlessNesting->Capital'})
            
        self.assertIn('Montgomery', read_json['Capital'])
        self.assertIn('Juneau', read_json['Capital'])
    
class JSONReadTest(unittest.TestCase):
    ''' Test that JSONs are being read correctly '''
    
    def test_read_flatten0(self):
        ''' Test if flatten=0 argument works '''
        
        read_json = pgreaper.read_json(file='data/us_states.json',
            flatten=None, extract={'State': 'State'})

        self.assertEqual(read_json['json'][0:5],
            [{"State": "Alabama", "Abbreviation": "AL"}, {"State": "Alaska", "Abbreviation": "AK"}, {"State": "Arizona", "Abbreviation": "AZ"}, {"State": "Arkansas", "Abbreviation": "AR"}, {"State": "California", "Abbreviation": "CA"}])
    
    # Commenting out because skipUnless() doesn't work
    # @unittest.skipUnless(PYTHON_VERSION >= 3.6, 'Dicts not ordered in <3.6')
    # def test_read_flatten1(self):
        # '''
        # Test if reading a "flat" JSON works file        
        # This test only passes 60% of the time for some odd reason
        # '''
        
        # # Should be equivalent due to similar structure
        # read_json = pgreaper.read_json(
            # path.join('data', 'us_states.json'), name='Countries')
        # read_csv = pgreaper.read_csv(
            # path.join('data', 'us_states.csv'), name='Countries')
        
        # self.assertEqual(read_json, read_csv)
        
    def test_read_flatten1a(self):
        ''' Like above, but is order-insensitive '''
        read_json = pgreaper.read_json(
            file=path.join('data', 'us_states.json'), name='Countries')
        read_csv = pgreaper.read_csv(
            file=path.join('data', 'us_states.csv'), name='Countries')
        
        self.assertEqual(read_json['Abbreviation'], read_csv['Abbreviation'])
        self.assertEqual(read_json['State'], read_csv['State'])

class JSONOutputTest(unittest.TestCase):
    ''' Test that JSONs are being outputted correctly '''
    
    @staticmethod
    def json_load(json_file):
        ''' Simple helper method which loads JSON files into dicts '''
        with open(json_file, 'r') as test_file:
            test_json = ''.join(test_file.readlines()).replace('\n', '')
        return json.loads(test_json)
        
    def test_output_txt(self):
        ''' Test if a simple TXT file is outputted correctly '''
        
        pgreaper.text_to_json('data/tab_delim.txt', 'tab_delim_test.json')
        output = self.json_load('tab_delim_test.json')
        self.assertEqual(output, [
            {"Capital": "Washington", "Country": "USA"},
            {"Capital": "Moscow", "Country": "Russia"},
            {"Capital": "Ottawa", "Country": "Canada"}
        ])
        
    def test_output_csv(self):
        '''
        Compare output vs a previously ouputted JSON which has been
        manually inspected for correctness
        '''
        
        pgreaper.csv_to_json('data/us_states.csv', 'us_states_test.json')
        test_json = self.json_load('us_states_test.json')
        compare_json = self.json_load('data/us_states.json')
        self.assertEqual(test_json, compare_json)
        
    def test_output_csv_zip(self):
        ''' Same as above but the input file is compressed '''
        
        zip_file = pgreaper.read_zip('data/us_states.zip')
        pgreaper.csv_to_json(zip_file['us_states.csv'], 'us_states_zip_test.json')
        
        test_json = self.json_load('us_states_zip_test.json')
        compare_json = self.json_load('data/us_states.json')
        self.assertEqual(test_json, compare_json)
        
    @classmethod
    def tearDownClass(cls):
        os.remove('tab_delim_test.json')
        os.remove('us_states_test.json')
        os.remove('us_states_zip_test.json')
        
# class SimplePGUpload(unittest.TestCase):
    # ''' Test that a simple JSON file is being uploaded correctly '''
    
    # @classmethod
    # def setUpClass(cls):
        # pgreaper.json_to_pg('')
        
    # @classmethod
    # def tearDown(cls):
        # os.remove('us_states_test.json')

if __name__ == '__main__':
    unittest.main()