import click
from click.testing import CliRunner

from pgreaper.cli import copy

# Command line tests not executing on Travis for some reason

# def test_read_print_zip():
    # ''' Test that printing out the contents of a ZIP file works '''
    # runner = CliRunner()
    # result = runner.invoke(copy, ['--zip', 'real_data/2016_Gaz_place_national.zip'])
    # assert result.exit_code == 0
    # assert '[0] 2016_Gaz_place_national.txt' in result.output
    
# def test_copy_zip_file_missing():
    # '''
    # Assert that an error is raised when user tries to upload a ZIP
    # archive without specifying which file
    # '''
    # runner = CliRunner()
    # result = runner.invoke(copy, ['--zip', '--csv',
        # 'real_data/2016_Gaz_place_national.zip'])
    # assert 'Please specify which file in the ZIP archive to upload' in result.output
    
#if __name__ == '__main__':
    # test_read_print_zip()
    # test_copy_zip_file_missing()