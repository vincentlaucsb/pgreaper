'''
Tests that the command line interface functions properly
'''

from sqlify.cli import menu

from click.testing import CliRunner
import os
import sqlite3
import unittest

class CLITest(unittest.TestCase):
    def test_exit(self):
        runner = CliRunner()
        result = runner.invoke(menu.cli, input='0\n')
        assert result.exit_code == 0

class SQLiteTest(unittest.TestCase):
    ''' Verify that creating an SQLite database through the CLI works '''
    
    def test_create_db(self):
        step1 = '2'  # Create SQL database
        step2 = 'data\SP500.csv'
        step3 = '2'  # Choose CSV
        step4 = ''   # Choose default option (',')
        step5 = 'y'  # Confirm delimiter is correct
        step6 = 'DATE' # Choose primary key
        step7 = ''   # Choose SQLite
        step8 = 'SP500.db'

        # TO DO: Strip out folder paths from default
        step9 = 'SP500'  # Choose table name
                    
        runner = CliRunner()

        result = runner.invoke(menu.cli,
            input='\n'.join([step1, step2, step3, step4, step5, step6, step7,
            step8, step9]) + '\n')
            
        conn = sqlite3.connect('SP500.db')
        data = conn.execute(
            "SELECT SP500 FROM SP500 WHERE DATE == '2007-03-12';").fetchall()[0][0]
        
        assert data == 1406.6
            
        conn.close()
            
    @classmethod
    def tearDownClass(cls):
        os.remove('SP500.db')

if __name__ == '__main__':
    unittest.main()