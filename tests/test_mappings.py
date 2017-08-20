''' Test Helper Mappings '''

from sqlify.core.mappings import *
import unittest

class CaseInsensitiveDictTest(unittest.TestCase):
    def setUp(self):
        self.d = CaseInsensitiveDict({
            'hello': 1,
            'world': 2
        })
        
    def test_lookup(self):
        self.assertEqual(self.d['HelLo'], 1)
        
    def test_delete(self):
        del self.d['HELLO']
        with self.assertRaises(KeyError):
            self.d['hello']
            
class SymmetricIndexTest(unittest.TestCase):
    def setUp(self):
        self.distances = SymmetricIndex()
        self.distances['San Francisco']['Oakland'] = 20
        self.distances['Paris']['Berlin'] = 200
        
        # Alternative value setting method
        self.distances['Chicago'] = {
            'Green Bay': 210
        }
        
    def test_lookup(self):
        self.assertEqual(self.distances['Berlin']['Paris'], 200)
        
    def test_delete(self):
        with self.assertRaises(NotImplementedError):
            del self.distances['Berlin']
            
    def test_correct_value(self):
        # Make sure non-sense doesn't happen
        with self.assertRaises(TypeError):
            self.distances['Chicago'] = 500
            
    def test_setitem(self):
        self.assertEqual(self.distances['Green Bay']['Chicago'], 210)
        
if __name__ == '__main__':
    unittest.main()