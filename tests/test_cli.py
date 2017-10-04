from click.testing import CliRunner
import click
import unittest

from pgreaper.cli import copy

class CLITest(unittest.TestCase):
    def test_read_print_zip(self):
        ''' Test that printing out the contents of a ZIP file works '''
        runner = CliRunner()
        result = runner.invoke(copy, ['--zip', 
            'csv-data/real_data/compressed/2015_StateDepartment.zip'])
            
        self.assertEqual(result.exit_code, 0)
        self.assertIn('[0] 2015_StateDepartment.csv', result.output)
        
    def test_copy_zip_file_missing(self):
        '''
        Assert that an error is raised when user tries to upload a ZIP
        archive without specifying which file
        '''
        runner = CliRunner()
        result = runner.invoke(copy, ['--zip', '--csv',
            'csv-data/real_data/compressed/2015_StateDepartment.zip'])
        self.assertIn('Please specify which file in the ZIP archive to upload',
            str(result))
    
if __name__ == '__main__':
    unittest.main()