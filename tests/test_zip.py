'''
Test ZipReader Interface
 * Uploading tests should be covered in other relevants tests
 * e.g. uploading compressed CSVs should be covered in test_csv.py
'''

import sqlify

import unittest
import os

class ZIPReaderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.zip_file = sqlify.read_zip('data/2015_StateDepartment.zip')
    
    def test_repr(self):
        ''' Spot check of string representation '''
        self.assertIn('[0] 2015_StateDepartment.csv',
            ZIPReaderTest.zip_file.__repr__())
    
    def test_index(self):
        ''' Assert that accessing files by index works '''
        
        self.assertEqual(ZIPReaderTest.zip_file[0].file,
            '2015_StateDepartment.csv')
            
    def test_key(self):
        ''' Assert that accesing files by key works '''
        
        self.assertEqual(ZIPReaderTest.zip_file['2015_StateDepartment.csv'].file,
            '2015_StateDepartment.csv')

    def test_key_error(self):
        '''
        Assert that trying to access a non-existant file gives an 
        appropriate error message
        '''
        
        with self.assertRaises(ValueError):
            ZIPReaderTest.zip_file['harambe.csv']
            
    def test_closed(self):
        ''' Assert that an error is raised when trying to read a closed file '''
        
        with ZIPReaderTest.zip_file[0] as infile:
            for line in infile:
                pass

        with self.assertRaises(ValueError):
            infile.read()
                
        with self.assertRaises(ValueError):
            infile.readline()
        
if __name__ == '__main__':
    unittest.main()