''' Tests for sqlify.settings() '''

from sqlify import config
import sqlify

import unittest
import os

class SettingsTest(unittest.TestCase):
    ''' Test if changing configuration settings works as advertised '''
    
    @classmethod
    def setUpClass(cls):
        # Get the original configuration file out of the way, if it exists
        try:
            os.rename(
                src=config.SQLIFY_CONF_PATH,
                dst=os.path.join(config.SQLIFY_PATH, 'config_orig.ini'))
        except FileNotFoundError:
            pass
    
    def test_first_config(self):
        ''' Test setting the configuration settings for the first time '''
        sqlify.settings(user='postgres', password='postgres', database='postgres')
        
        self.assertEqual(
            config.SQLIFY_CONF['postgres_default']['user'], 'postgres')
        self.assertEqual(
            config.SQLIFY_CONF['postgres_default']['password'], 'postgres')
        self.assertEqual(
            config.SQLIFY_CONF['postgres_default']['dbname'], 'postgres')
        self.assertEqual(
            config.SQLIFY_CONF['postgres_default']['host'], 'localhost')
    
    def test_second_config(self):
        ''' Test modifying configuration settings after the first time '''
        
        sqlify.settings(user='peytonmanning', pw='omaha')
        # sqlify.settings(username='peytonmanning', pw='omaha')
        
        self.assertEqual(
            config.SQLIFY_CONF['postgres_default']['user'], 'peytonmanning')
        self.assertEqual(
            config.SQLIFY_CONF['postgres_default']['password'], 'omaha')
    
    @unittest.skip("Not important right now")
    def test_default_settings(self):
        ''' Test that the DefaultSettings class works as advertised '''
        
        omaha = sqlify.config.DefaultSettings('postgres_default')
        set_hut = omaha(dbname='broncos')

        self.assertEqual(set_hut['dbname'], 'broncos')
        self.assertEqual(set_hut['user'], 'peytonmanning')
        self.assertEqual(omaha['dbname'], 'postgres')
        
    @classmethod
    def tearDownClass(cls):
        # Restore original configuration file
        os.remove(config.SQLIFY_CONF_PATH)
        
        try:
            os.rename(
                src=os.path.join(config.SQLIFY_PATH, 'config_orig.ini'),
                dst=config.SQLIFY_CONF_PATH)
        except FileNotFoundError:
            pass
    
if __name__ == '__main__':
    unittest.main()