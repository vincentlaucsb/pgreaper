'''
Walks user through creating an SQL database

==== TO DO ====
 * Fix error when choosing a primary key
 * Give user ability to choose a header row
'''

from sqlify import sqlify
from sqlify import utils
from sqlify import settings
from sqlify.cli.helpers import echo_hrule, echo_trim, echo_table, validate_prompt
from sqlify.helpers import _strip

import click
import os

CURRENT_PATH = os.getcwd()

# To call, type `sqlify` from the command line
@click.command()
def cli():
    click.echo('Welcome to SQLify Version 1.0.1')
    click.echo('')
    
    echo_hrule()
    click.echo('PostgresSQL Configuration Settings (from settings.py)')
    click.echo('Default User: ' + settings.POSTGRES_DEFAULT_USER)
    click.echo('Default Database: ' + settings.POSTGRES_DEFAULT_DATABASE)
    click.echo('Default Host: ' + settings.POSTGRES_DEFAULT_HOST)
    click.echo('Default Password: ' + '*'*len(settings.POSTGRES_DEFAULT_PASSWORD))
    echo_hrule()
    click.echo()
    
    click.echo('[1] Read the first few lines of a file')
    click.echo('[2] Convert a file to SQL')
    click.echo('...\n'*2)
    click.echo('[0] Quit')
    click.echo()
    
    while True:
        response = click.prompt('Please enter an number to continue')
    
        if response.isnumeric():
            if response == '1':
                preview()
            elif response == '2':
                to_sql()
                
            break
        else:
            click.echo('Invalid response. Enter "0" or Ctrl + C to quit.')
            click.echo('\n')
            
def to_sql(filename=None):
    if not filename:
        filename = click.prompt(
            'Please enter a file (relative to {0})'.format(CURRENT_PATH),
            type=click.Path(exists=True))

    file_info = get_file_info(filename)
    delimiter = file_info.delimiter

    # Database engine optins
    engines = {'1': 'sqlite', '2': 'postgres'}
    engine_choice = engines[validate_prompt(
        prompt = ['Which database engine would you like to use?',
                  '[1] SQLite',
                  '[2] PostgresSQL',
                  'Enter a number'],
        valid = engines.keys(),
        default='1'
    )]
    
    # Choose save location
    database = click.prompt('Enter a database to save to')
    table_name = click.prompt('What do you want to name your table\n' \
                              '(Enter nothing to use default)',
                              default=_strip(filename.split('.')[0]))
    
    # Create the database
    pg_settings = {}
    
    if engine_choice == 'postgres':
        pg_settings = postgres_settings()
        
    arg_dict = {
        'file': filename,
        'name': table_name,
        'delimiter': delimiter,
        'database': os.path.join(CURRENT_PATH, database),
        'engine': engine_choice,
    }
    
    # Use text_to_sql or csv_to_sql depending on the file type
    if file_info.file_type == 'txt':
        sqlify.text_to_sql(**arg_dict, **pg_settings)
    else:
        sqlify.csv_to_sql(**arg_dict, **pg_settings)
    
    click.echo('Created SQL database')
    
def preview():
    filename = click.prompt('What is the filename?', type=click.Path(exists=True))
    
    # Give a short preview
    for i in utils.preview(filename, n=10):
        click.echo(i)
    
    # Ask for follow-up
    option = click.prompt('Would you like to convert this file to SQL? [y/n]')
    
    if option.lower() == 'y':
        to_sql(filename)
    else:
        quit()
        
# Get necessary file information
def get_file_info(filename):
    def get_delim():
        delim = click.prompt('How is the data delimited (separated)?',
                              default=default_delim)
                              
        # Give a short preview
        click.echo('')
        echo_hrule('Table of {0}'.format(filename))
        click.echo(utils.head(filename, type=file_type, delimiter=delim))
        echo_hrule('End of table')
        click.echo('')
        
        echo_trim('This is what a tabular representation of {0} '.format(filename) \
              + 'looks like assuming the choice of delimiter was correct. ' \
              + 'If the table looks malformed, then the wrong delimiter ' \
              + 'was probably chosen.')
        
        # Reprompt user if unsatisfied
        if click.prompt('Is this delimiter correct? [y/n]').lower() == 'y':
            return delim
        else:
            get_delim()
    
    def get_primary_key():
        ''' Prompt user about which column is the primary key '''
        
        click.echo('')
        
        return validate_prompt(
            prompt=[echo_hrule('Begin column names'), '',
                    echo_table(tbl.col_names), '',
                    echo_hrule('End column names'),
                    'Which column do you want as the primary key?',
                    "(Enter 0 if you don't know/want/have one)"],
            valid=tbl.col_names + [0])

    # Text or CSV/DSV?
    file_choice = validate_prompt(
        prompt=['What type of data file is this?',
                '[1] Text file',
                '[2] CSV (comma-separated values)',
                '[3] TSV (tab-separated values)',
                '[4] DSV (any file separated by some delimiter, e.g. pipe "|")\n',
                'Enter a number'],
        valid=[1, 2, 3, 4])
    
    # Try to guess the right delimiter
    if file_choice == '1':
        file_type='text'
        default_delim = ' '
    else:
        file_type = 'csv'
        
        if file_choice == '2':
            default_delim = ','
        elif file_choice == '3':
            default_delim = '\t'
        else:
            default_delim = None
    
    delimiter = get_delim()
    
    tbl = utils.head(filename, type=file_type, delimiter=delimiter)
    tbl.delimiter = delimiter
    tbl.file_type = file_type
    tbl.p_key = get_primary_key()
    
    return tbl
    
# Prompt user for input on connecting to a Postgres database
def postgres_settings():
    username = click.prompt(
        "What username do you want to connect with?\n" +
        "(Leave blank to use {0})".format(settings.POSTGRES_DEFAULT_USER),
        default=settings.POSTGRES_DEFAULT_USER)
        
    password = click.prompt(
        "What password do you want to connect with?\n" +
        "(Leave blank to use default)",
        hide_input=True, confirmation_prompt=True,
        default=settings.POSTGRES_DEFAULT_PASSWORD)
        
    return {'username': username, 'password': password}