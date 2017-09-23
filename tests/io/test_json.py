# ''' Tests for converting files to JSON '''

# from pgreaper.testing import *
# import pgreaper

# import json
# import sys

# PYTHON_VERSION = sys.version_info[0] + 0.1 * sys.version_info[1] + \
    # sys.version_info[2]
    
# class PyJSONTest(unittest.TestCase):
    # ''' Test that JSON objects in Python are being read correctly '''
    
    # # Load fake persons data
    # with open(path.join(MIMESIS_DIR, 'persons.json'), mode='r') as infile:
        # person_data = ''.join(infile.readlines())
        
    # def test_filter(self):
        # ''' Test if filter argument works '''
        
        # read_json = pgreaper.read_json(PyJSONTest.person_data,
            # filter=['full_name', 'age', 'occupation'])

        # self.assertEqual(len(read_json['full_name']), 50000)
        # self.assertEqual(len(read_json['age']), 50000)
        # self.assertEqual(len(read_json['occupation']), 50000)
        
    # def test_extract_nested(self):
        # ''' Test that extract argument works for nested dicts '''
        
        # json_data = [{
            # "State": "Alabama",
            # "Abbreviation": "AL",
            # "PointlessNesting": {
                # "Capital": "Montgomery"
        # }}, {
            # "State": "Alaska",
            # "Abbreviation": "AK",
            # "PointlessNesting": {
                # "Capital": "Juneau"
        # }}, {
            # "State": "Arizona", "Abbreviation": "AZ"},
        # {"State": "Arkansas", "Abbreviation": "AR"},
        # {"State": "California", "Abbreviation": "CA"}]
        
        # read_json = pgreaper.read_json(json_data,
            # flatten='outer', extract={'Capital': 'PointlessNesting->Capital'})
            
        # self.assertIn('Montgomery', read_json['Capital'])
        # self.assertIn('Juneau', read_json['Capital'])
    
# class JSONReadTest(unittest.TestCase):
    # ''' Test that JSONs are being read correctly '''
    
    # def test_read_flatten0(self):
        # ''' Test if flatten=0 argument works '''
        
        # read_json = pgreaper.read_json(file='data/us_states.json',
            # flatten=None, extract={'State': 'State'})

        # self.assertEqual(read_json['json'][0:5],
            # [{"State": "Alabama", "Abbreviation": "AL"}, {"State": "Alaska", "Abbreviation": "AK"}, {"State": "Arizona", "Abbreviation": "AZ"}, {"State": "Arkansas", "Abbreviation": "AR"}, {"State": "California", "Abbreviation": "CA"}])
    
    # # Commenting out because skipUnless() doesn't work
    # # @unittest.skipUnless(PYTHON_VERSION >= 3.6, 'Dicts not ordered in <3.6')
    # # def test_read_flatten1(self):
        # # '''
        # # Test if reading a "flat" JSON works file        
        # # This test only passes 60% of the time for some odd reason
        # # '''
        
        # # # Should be equivalent due to similar structure
        # # read_json = pgreaper.read_json(
            # # path.join('data', 'us_states.json'), name='Countries')
        # # read_csv = pgreaper.read_csv(
            # # path.join('data', 'us_states.csv'), name='Countries')
        
        # # self.assertEqual(read_json, read_csv)
        
    # @unittest.skip("Deprecated")
    # def test_read_flatten1a(self):
        # ''' Like above, but is order-insensitive '''
        # read_json = pgreaper.read_json(
            # file=path.join('data', 'us_states.json'), name='Countries')
        # read_csv = pgreaper.read_csv(
            # file=path.join('data', 'us_states.csv'), name='Countries')
        
        # self.assertEqual(read_json['Abbreviation'], read_csv['Abbreviation'])
        # self.assertEqual(read_json['State'], read_csv['State'])

# if __name__ == '__main__':
    # unittest.main()