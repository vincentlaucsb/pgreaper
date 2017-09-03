''' Test of _unnest() Function '''

import pgreaper
from pgreaper.testing import *
from pgreaper.postgres.loader import _unnest

import unittest

class UnnestTest(unittest.TestCase):
    def test_simple_case(self):
        table = world_countries_table()
        table.guess_type()
        
        unnest = _unnest(table)
        self.assertEqual(unnest[0],
            "unnest(ARRAY['Washington', 'Moscow', 'Ottawa']::text[])")
        self.assertEqual(unnest[-1],
            "unnest(ARRAY[324774000, 144554993, 35151728]::bigint[])")
            
    def test_jsonb(self):
        table = world_countries_table()
        table.add_col('Metadata', fill={
            'Planet': 'Earth',
            'Hemisphere': 'Northern'})
        table.guess_type()
        
        unnest = _unnest(table)
        correct =  '''unnest(ARRAY['{"Planet": "Earth", "Hemisphere": "Northern"}', '{"Planet": "Earth", "Hemisphere": "Northern"}', '{"Planet": "Earth", "Hemisphere": "Northern"}']::jsonb[])'''
        
        self.assertEqual(unnest[-1], correct)

if __name__ == '__main__':
    unittest.main()