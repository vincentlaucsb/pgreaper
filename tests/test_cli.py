from click.testing import CliRunner
import click
import unittest

from pgreaper.cli import copy

class CLITest(unittest.TestCase):
    # Make tests a subclass of CLITest to fix the issue of tests
    # not running on Travis CI
    
    def test_read_print_zip(self):
        ''' Test that printing out the contents of a ZIP file works '''
        runner = CliRunner()
        result = runner.invoke(copy, ['--zip', 
            'csv-data/real_data/compressed/2015_StateDepartment.zip'])
            
        assert result.exit_code == 0
        assert '[0] 2015_StateDepartment.csv' in result.output
        
    def test_copy_zip_file_missing(self):
        '''
        Assert that an error is raised when user tries to upload a ZIP
        archive without specifying which file
        '''
        runner = CliRunner()
        result = runner.invoke(copy, ['--zip', '--csv',
            'csv-data/real_data/compressed/2015_StateDepartment.zip'])
        assert 'Please specify which file in the ZIP archive to upload' in str(result)
    
if __name__ == '__main__':
    unittest.main()