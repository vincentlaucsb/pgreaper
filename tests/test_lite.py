''' Test the small things '''

import sqlify
import unittest

class SingletonTest(unittest.TestCase):
    ''' Makes sure that singletons are really singletons '''

    def test_single_dialect(self):
        x = sqlify.core.schema.DialectSQLite()
        y = sqlify.core.schema.DialectSQLite()
        
        self.assertEqual(id(x), id(y))
        
if __name__ == '__main__':
    unittest.main()