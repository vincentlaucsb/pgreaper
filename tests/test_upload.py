''' Test if SQLUploader's helpers work ok '''

from sqlify.core.uploader import SQLUploader

from functools import partial
import unittest

class TestWiden(unittest.TestCase):
    '''
    Make sure that the widen() method is returning the correct schema
    '''

    uploader = SQLUploader
    widen_input = partial(uploader.widen,
        input_cols=['thanks', 'obama'],
        input_types=['text', 'text'],
        sql_cols=['thanks', 'alot', 'obama'],
        sql_types=['text', 'text', 'text'])
            
    widen_sql = partial(uploader.widen,
        input_cols=['420', 'blaze', 'it'],
        input_types=['text', 'text', 'text'],
        sql_cols=['420', 'blaze'],
        sql_types=['text', 'text'])
        
    reorder_only = partial(uploader.widen,
        reorder=True,
        input_cols=['thanks', 'obama', 'alot'],
        input_types=['text', 'text', 'text'],
        sql_cols=['thanks', 'alot', 'obama'],
        sql_types=['text', 'text', 'text']
    )

    def test_widen_input(self):
        self.assertEqual(TestWiden.widen_input(add_cols = True),
            [('thanks', 'text'),
             ('alot', 'text'),
             ('obama', 'text')])
    
    def test_no_widen_input(self):
        self.assertEqual(TestWiden.widen_input(add_cols = False), None)
        
    def test_widen_sql(self):
        self.assertEqual(TestWiden.widen_sql(add_cols = True),
            [('420', 'text'),
             ('blaze', 'text'),
             ('it', 'text')])
        
    def test_no_widen_sql(self):
        self.assertEqual(TestWiden.widen_sql(add_cols = False), None)
        
    def test_reorder(self):
        self.assertEqual(TestWiden.reorder_only(add_cols = True),
            [('thanks', 'text'),
             ('alot', 'text'),
             ('obama', 'text')])
        
    def test_reorder_no_widen(self):
        self.assertEqual(TestWiden.reorder_only(add_cols = False, reorder=True),
            [('thanks', 'text'),
             ('alot', 'text'),
             ('obama', 'text')])
             
    def test_no_reorder_no_widen(self):
        self.assertEqual(TestWiden.reorder_only(add_cols = False, reorder=False),
            None)
        
if __name__ == '__main__':
    unittest.main()